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
	log.Println("Create main table")
	//@REF https://forums.aws.amazon.com/thread.jspa?threadID=119125
	w.app.DB.CreateBillTable("aws_billing_" + payload.ProjectID)
	dropQuery := fmt.Sprintf("ALTER TABLE aws_billing_%s DROP COLUMN  IF EXISTS \"user:cluster\" RESTRICT", payload.ProjectID)
	w.app.DB.Query(dropQuery)

	job.UpdateStatus("copy temp table")
	log.Println("Start create temporary table")

	w.app.DB.CreateBillTable("_job_" + job.ID)
	q := fmt.Sprintf(`COPY _job_%s
	FROM 's3://%s/%s'
	credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
	CSV
	IGNOREHEADER 1
	ssh
	TRUNCATECOLUMNS;`, job.ID, manifestBucket, manifest, aws.Key, aws.Secret)
	rows, err := w.app.DB.Query(q)

	log.Println("Drop extra column")
	dropQuery = fmt.Sprintf("ALTER TABLE _job_%s DROP COLUMN IF EXISTS \"user:cluster\" RESTRICT", job.ID)
	w.app.DB.Query(dropQuery)

	q = fmt.Sprintf(`UPDATE _job_%s
	SET recordid=concat('%s', random())
	WHERE recordid=''`, job.ID, payload.GetFilename())
	log.Println("Generate temp recordid %s", q)

	log.Println("Delete old row in current table")
	q = fmt.Sprintf(`DELETE FROM aws_billing_%s
	WHERE recodid LIKE '%s%'`, payload.ProjectID, payload.GetFilename())
	rows, err = w.app.DB.Query(q)

	log.Println("Merge temp table to main table")
	q = fmt.Sprintf(`delete from aws_billing_%s
	using _job_%s
	where aws_billing_%s.recordid = _job_%s.recordid
	`)
	rows, err = w.app.DB.Query(q)
	q = fmt.Sprintf(`insert into aws_billing_%s
	select * from _job_%s`, payload.ProjectID, job.ID)
	rows, err = w.app.DB.Query(q)

	log.Println("Drop temp table")
	w.app.DB.Query(fmt.Sprintf("DROP TABLE _job_%s", job.ID))

	if err != nil {
		log.Printf("err write %v. Rows %v", err, rows)
		job.UpdateStatus(fmt.Sprintf("Fail. Err %v %v", err, rows))
		return
	}
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
