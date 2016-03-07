package main

import (
	//"bytes"
	"crypto/md5"
	"crypto/rand"
	//"encoding/base64"
	"log"
	//"strings"
	"fmt"
)

type Queue struct {
	Size    int
	JobChan chan *Job
}

func (q *Queue) init() {
	q.JobChan = make(chan *Job, q.Size)
}

func genJobId() string {
	c := 40
	b := make([]byte, c)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	//rs := strings.Replace(base64.URLEncoding.EncodeToString(b), "/", "0", -1)
	return fmt.Sprintf("%x", md5.Sum(b))
}

func (q *Queue) Push(p *Payload) (string, error) {
	job := &Job{p, genJobId()}
	log.Println("Push job")
	q.JobChan <- job
	log.Println("Done push job")
	return job.ID, nil
}

func (q *Queue) Pop() *Job {
	j := <-q.JobChan
	return j
}
