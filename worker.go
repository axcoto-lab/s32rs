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
	Size int
	app  *App
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
	job.UpdateStatus("update manifest")
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
	dropQuery := fmt.Sprintf("ALTER TABLE aws_billing_%s DROP \"user:cluster\" RESTRICT", payload.ProjectID)
	log.Println("Drop column query %s", dropQuery)
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
	dropQuery = fmt.Sprintf("ALTER TABLE _job_%s DROP \"user:cluster\" RESTRICT", job.ID)
	w.app.DB.Query(dropQuery)

	q = fmt.Sprintf(`UPDATE _job_%s
	SET recordid=concat('%s', random())
	WHERE recordid=''`, job.ID, payload.GenerateRecordIDPrefix())
	log.Printf("Generate temp recordid %s\n", q)
	w.app.DB.Query(q)

	q = fmt.Sprintf(`DELETE FROM aws_billing_%s
	WHERE recordid LIKE '%s%%'`, payload.ProjectID, payload.GenerateRecordIDPrefix())
	log.Printf("Delete generate record id from main table %s\n", q)
	rows, err = w.app.DB.Query(q)

	job.UpdateStatus("merge temp table")
	log.Println("Merge temp table to main table")
	q = fmt.Sprintf(`DELETE FROM aws_billing_%s
	USING _job_%s
	WHERE aws_billing_%s.recordid = _job_%s.recordid
	`, payload.ProjectID, job.ID, payload.ProjectID, job.ID)
	log.Printf("Delete old row in current table %s\n", q)
	rows, err = w.app.DB.Query(q)

	q = fmt.Sprintf(`INSERT INTO aws_billing_%s
	SELECT * FROM _job_%s`, payload.ProjectID, job.ID)
	log.Printf("Insert from main table %s\n", q)
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
		for i := 1; i <= w.Size; i++ {
			go w.perform(job)
		}
	}
}
