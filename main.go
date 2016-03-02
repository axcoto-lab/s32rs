package main

import (
	//"bytes"
	"crypto/rand"
	"encoding/base64"
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

type Job struct {
	ID string
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

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/work", WorkHandler)
	r.HandleFunc("/job/{id}", JobHandler)
	http.Handle("/", r)

	http.ListenAndServe(":3001", r)
}

func genJobId() string {
	c := 40
	b := make([]byte, c)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("error:", err)
		return ""
	}
	rs := strings.Replace(base64.URLEncoding.EncodeToString(b), "/", "0", -1)
	return rs
}

func WorkHandler(w http.ResponseWriter, r *http.Request) {
	//vars := mux.Vars(r)

	p := &Payload{
		ProjectID: r.FormValue("project_id"),
		AwsKey:    r.FormValue("aws_key"),
		AwsSecret: r.FormValue("aws_secret"),
		S3Bucket:  r.FormValue("s3_bucket"),
	}

	jobId := genJobId()

	fmt.Fprintf(w, "%s", jobId)
	go doWork(jobId, p)
}

func JobHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobId := vars["id"]
	job := Job{
		ID: jobId,
	}

	fmt.Fprintf(w, "%s", job.GetStatus())
}
