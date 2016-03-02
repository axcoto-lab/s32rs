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

func fetchS3(from, to string, aws *AwsKey) {
}

func copyToRS(jobId string, payload *Payload, dbinfo string, manifestBucket string, aws *AwsKey) {
	log.Printf("Fetch data fro s3 source")
	csvSource := fmt.Sprintf("/s32rs/%s_%s", jobId, payload.GetFilename())

	cmdArgs := []string{"s3", "cp",
		fmt.Sprintf("s3://%s", payload.S3Bucket),
		csvSource,
		"--source-region", "us-east-1"}

	cmd := exec.Command("/usr/local/bin/aws", cmdArgs...)

	env := os.Environ()
	env = append(env, fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", payload.AwsKey), fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", payload.AwsSecret))
	cmd.Env = env

	log.Printf("Waiting for S3 download to finish...")
	if cmdOut, err := cmd.Output(); err != nil {
		log.Println("Command finished with error: %v", err)
		fmt.Fprintln(os.Stderr, err)
		return
	} else {
		log.Printf("Command Output %s\n", cmdOut)
	}

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

		cmdArgs := []string{"s3", "cp",
			manifestSource,
			fmt.Sprintf("s3://%s/%s", manifestBucket, manifest),
			"--source-region", "us-east-1"}

		cmd := exec.Command("/usr/local/bin/aws", cmdArgs...)

		env := os.Environ()
		env = append(env, fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", aws.Key), fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", aws.Secret))
		cmd.Env = env

		log.Printf("Waiting for Manifest uploading...")
		if cmdOut, err := cmd.Output(); err != nil {
			log.Println("Command finished with error: %v", err)
			fmt.Fprintln(os.Stderr, err)
			return
		} else {
			log.Printf("Command Output %s\n", cmdOut)
		}
	}
	log.Printf("Manifest preparing is done")

	log.Printf("DBInfo %s\n", dbinfo)

	db, err := sql.Open("postgres", dbinfo)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Start copy")
	q := fmt.Sprintf(`COPY vinh_test1
	FROM 's3://%s/%s'
	credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
	CSV
	IGNOREHEADER 1
	ssh`, manifestBucket, manifest, aws.Key, aws.Secret)
	rows, err := db.Query(q)
	log.Println("Will run %s\n", q)

	defer rows.Close()
	log.Println("Done copy")

	if rows != nil {
		for rows.Next() {
			var count int
			err = rows.Scan(&count)

			log.Printf("Loaded %f", count)
		}
	}
}
