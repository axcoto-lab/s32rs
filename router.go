package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

type Payload struct {
	ProjectID string
	AwsKey    string
	AwsSecret string
	S3Bucket  string
}

func (p *Payload) GetFilename() string {
	parts := strings.Split(p.S3Bucket, "/")
	return parts[len(parts)-1]
}

func (p *Payload) GenerateRecordIDPrefix() string {
	s := p.GetFilename()
	s = strings.Replace(s, "aws-billing-detailed-line-items-with-resources-and-tags-", "", -1)
	s = strings.Replace(s, ".csv", "", -1)
	s = strings.Replace(s, ".zip", "", -1)
	s = strings.Replace(s, ".gzip", "", -1)
	s = strings.Replace(s, ".gz", "", -1)
	return s + "-"
}

type Job struct {
	Payload *Payload
	ID      string
}

func (job *Job) GetStatus() string {
	dat, err := ioutil.ReadFile("/s32rs/status_" + job.ID)
	if err != nil {
		return "unknow"
	}
	return string(dat)
}

func (job *Job) UpdateStatus(status string) {
	f, err := os.Create("/s32rs/status_" + job.ID)
	if err != nil {
		log.Printf("Cannot create status file")
	}
	defer f.Close()
	if _, err := f.WriteString(status); err != nil {
		log.Printf("Error when update status %s %v", job.ID, err)
	}
	f.Sync()
}

type WorkHandler struct {
	app *App
}

func (h *WorkHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//vars := mux.Vars(r)

	p := &Payload{
		ProjectID: r.FormValue("project_id"),
		AwsKey:    r.FormValue("aws_key"),
		AwsSecret: r.FormValue("aws_secret"),
		S3Bucket:  r.FormValue("s3_bucket"),
	}

	jobId, err := h.app.Qe.Push(p)
	if err != nil {
		//@TODO Return http error code
		fmt.Fprintf(w, "Cannot create job")
	} else {
		fmt.Fprintf(w, "%s", jobId)
	}
}

type JobHandler struct {
}

func (H *JobHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobId := vars["id"]
	job := Job{
		ID: jobId,
	}

	fmt.Fprintf(w, "%s", job.GetStatus())
}
