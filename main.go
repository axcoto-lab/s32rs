package main

import (
	//"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
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

	fmt.Fprintf(w, "%s", jobId)
}
