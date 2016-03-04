package main

import (
	//"bytes"
	"crypto/rand"
	"encoding/base64"
	"strings"
)

type Queue struct {
	Size        int
	q           []string
	JobChan     chan string
	ControlChan chan string
}

func (q *Queue) init() {
	q.q = make([]string, q.Size, q.Size)
	q.JobChan = make(chan string)
	q.ControlChan = make(chan string)
}

func genJobId() string {
	c := 40
	b := make([]byte, c)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	rs := strings.Replace(base64.URLEncoding.EncodeToString(b), "/", "0", -1)
	return rs
}
func (q *Queue) Push(p *Payload) (string, error) {
	jobId := genJobId()
	go doWork(jobId, p)
	return jobId, nil
}

func (q *Queue) start() {

	for {
		select {
		case sig := <-q.ControlChan:
			if sig == "STOP" {
				return
			}
		case job := <-q.JobChan:
			q.q[len(q.q)+1] = job
		}
	}
}
