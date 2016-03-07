package main

import (
	//"database/sql"
	"fmt"
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

type Worker struct {
	app *App
}

func (w *Worker) perform(job *Job) {
	w.copyToRS(job, os.Getenv("AWS_BUCKET_S32RS"), &AwsKey{os.Getenv("AWS_KEY"), os.Getenv("AWS_SECRET")})
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

func (w *Worker) copyToRS(job *Job, manifestBucket string, aws *AwsKey) {
	jobId := job.ID
	payload := job.Payload

	job.UpdateStatus("pending")

	log.Printf("Fetch data from s3 source")
	csvSource := fmt.Sprintf("/s32rs/%s_%s", jobId, payload.GetFilename())

	cpS3(fmt.Sprintf("s3://%s", payload.S3Bucket),
		csvSource, &AwsKey{payload.AwsKey, payload.AwsSecret}, []string{})
	log.Printf("Done fetch data from s3 source")

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
	log.Printf("Done Prepare manifest file")

	log.Println("Start copy")
	log.Println("Start drop old table")
	job.UpdateStatus("drop")
	w.app.DB.Query(fmt.Sprintf("DROP TABLE aws_billing_%s", payload.ProjectID))
	log.Println("Start creatabe table schema")
	job.UpdateStatus("create")
	//@REF https://forums.aws.amazon.com/thread.jspa?threadID=119125
	schemaQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS aws_billing_%s  (
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
    "user:cluster" char(1))`, "aws_billing_"+payload.ProjectID)
	w.app.DB.Query(schemaQuery)
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
	rows, err := w.app.DB.Query(q)
	log.Println("Query run %s\n", q)
	if err != nil {
		log.Printf("err write %v. Rows %v", err, rows)
		job.UpdateStatus(fmt.Sprintf("Fail. Err %v %v", err, rows))
		return
	}

	log.Println("Drop extra column")
	dropQuery := fmt.Sprintf("ALTER TABLE aws_billing_%s DROP COLUMN \"user:cluster\" RESTRICT", payload.ProjectID)
	w.app.DB.Query(dropQuery)

	log.Println("Done copy")
	job.UpdateStatus("done")
}

func (w *Worker) Work() {
	q := w.app.Qe

	for {
		job := <-q.JobChan
		log.Println("Process job %v", job)
		go w.perform(job)
	}
}
