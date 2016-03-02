package main

import (
	//"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

type Payload struct {
	ProjectID string
	AwsKey    string
	AwsSecret string
	S3Bucket  string
}

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/work", WorkHandler)
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
	rs := base64.URLEncoding.EncodeToString(b)
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

	fmt.Fprintf(w, "Payload %v. JobID: %v\n", p, jobId)
	//fmt.Fprintf(w, jobId)
	go doWork(jobId, p)
}
