package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	//"io/ioutil"

	"log"
	"os"
	"os/exec"
)

const manifestTmpl = `{
  "entries": [
       {"endpoint":"%s",
          "command": "unzip -p %s | cat",
          "mandatory":true,
          "username": "%s"}
  ]
}`

type AwsKey struct {
	Key    string
	Secret string
}

func doWork(jobId string, payload *Payload) {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		os.Getenv("PG_USER"),
		os.Getenv("PG_PWD"),
		os.Getenv("PG_DB"),
		os.Getenv("PG_HOST"),
		os.Getenv("PG_PORT"))

	copyToRS(jobId, payload, dbinfo,
		os.Getenv("AWS_BUCKET_S32RS"), &AwsKey{os.Getenv("AWS_KEY"), os.Getenv("AWS_SECRET")})
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func cpS3(from, to string, aws *AwsKey, envs []string) {
	cmdArgs := []string{"s3", "cp",
		from,
		to,
		"--source-region", "us-east-1"}

	cmd := exec.Command("/usr/local/bin/aws", cmdArgs...)

	env := os.Environ()
	env = append(env, envs...)
	env = append(env, fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", aws.Key), fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", aws.Secret))
	cmd.Env = env

	log.Printf("Waiting for S3 download to finish...")
	if cmdOut, err := cmd.Output(); err != nil {
		log.Println("Command finished with error: %v", err)
		fmt.Fprintln(os.Stderr, err)
		return
	} else {
		log.Printf("Command Output %s\n", cmdOut)
	}

}

func copyToRS(jobId string, payload *Payload, dbinfo string, manifestBucket string, aws *AwsKey) {
	job := Job{jobId}
	job.UpdateStatus("pending")

	log.Printf("Fetch data fro s3 source")
	csvSource := fmt.Sprintf("/s32rs/%s_%s", jobId, payload.GetFilename())

	cpS3(fmt.Sprintf("s3://%s", payload.S3Bucket),
		csvSource, &AwsKey{payload.AwsKey, payload.AwsSecret}, []string{})

	log.Printf("Extract data and rezip in gzip")
	//exe.Commnad("unzip",
	log.Printf("Extract is done")

	log.Printf("Prepare manifest file")
	manifest := fmt.Sprintf("manifest_%s.json", jobId)
	manifestSource := fmt.Sprintf("/s32rs/%s", manifest)
	f, err := os.Create(manifestSource)
	defer f.Close()
	f.Sync()
	if _, err := f.WriteString(fmt.Sprintf(manifestTmpl, os.Getenv("SSH_IP"), csvSource, os.Getenv("SSH_USER"))); err == nil {
		log.Printf("Manifest content %s", fmt.Sprintf(manifestTmpl, os.Getenv("SSH_IP"), csvSource, os.Getenv("SSH_USER")))

		cpS3(manifestSource,
			fmt.Sprintf("s3://%s/%s", manifestBucket, manifest),
			aws,
			[]string{})
	}
	log.Printf("Manifest preparing is done")

	log.Printf("DBInfo %s\n", dbinfo)

	db, err := sql.Open("postgres", dbinfo)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Start copy")
	log.Println("Start drop old table")
	job.UpdateStatus("drop")
	db.Query(fmt.Sprintf("DROP TABLE aws_billing_%s", payload.ProjectID))
	log.Println("Start creatabe table schema")
	job.UpdateStatus("create")
	//@REF https://forums.aws.amazon.com/thread.jspa?threadID=119125
	schemaQuery := fmt.Sprintf(`CREATE TABLE aws_billing_%s  (
    invoiceid character varying(256),
    payeraccountid character varying(256),
    linkedaccountid character varying(256),
    recordtype character varying(256),
    recordid character varying(256),
    productname character varying(256),
    rateid character varying(256),
    subscriptionid character varying(256),
    pricingplanid character varying(256),
    usagetype character varying(256),
    operation character varying(256),
    availabilityzone character varying(256),
    reservedinstance character varying(256),
    itemdescription character varying(256),
    usagestartdate character varying(256),
    usageenddate character varying(256),

		usagequantity numeric(38, 16) NULL,
		rate numeric(38, 16) NULL,
		cost numeric(38, 16) NULL,

    resourceid character varying(256),
    "user:cluster" char(1))`, payload.ProjectID)
	db.Query(schemaQuery)
	log.Printf("Schema: %s", schemaQuery)

	job.UpdateStatus("copy")
	log.Println("Execute copy command")
	q := fmt.Sprintf(`COPY aws_billing_%s
	FROM 's3://%s/%s'
	credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
	CSV
	IGNOREHEADER 1
	ssh
	TRUNCATECOLUMNS;`, payload.ProjectID, manifestBucket, manifest, aws.Key, aws.Secret)
	rows, err := db.Query(q)
	log.Println("Query run %s\n", q)
	if err != nil {
		log.Printf("err write %v. Rows %v", err, rows)
		job.UpdateStatus(fmt.Sprintf("Fail. Err %v %v", err, rows))
		return
	}

	log.Println("Drop extra column")
	dropQuery := fmt.Sprintf("ALTER TABLE aws_billing_%s DROP COLUMN \"user:cluster\" RESTRICT", payload.ProjectID)
	db.Query(dropQuery)

	log.Println("Done copy")
	job.UpdateStatus("done")
}
