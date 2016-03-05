package main

import (
	"github.com/gorilla/mux"
)

func main() {
	r := mux.NewRouter()

	qe := initQueue()
	db := initDB()

	r.HandleFunc("/work", WorkHandler(db, qe))
	r.HandleFunc("/job/{id}", JobHandler)

	initServer(r)
}
