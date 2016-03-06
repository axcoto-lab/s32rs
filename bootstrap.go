package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"os"
)

type app struct {
	Qe *Queue
	DB *DB
	R  *mux.Router
}

func (a *app) init() {
	router := mux.NewRouter()

	router.Handle("/work", &WorkHandler{a.DB, a.Qe})
	router.Handle("/job/{id}", &JobHandler{})
	a.R = router
}

func initServer(r *app) {
	http.Handle("/", r.R)
	http.ListenAndServe(":3001", r.R)
}

func initQueue() *Queue {
	return &Queue{
		Size: 100,
	}
}

func initDB() *DB {
	return &DB{
		ConnString: fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
			os.Getenv("PG_USER"),
			os.Getenv("PG_PWD"),
			os.Getenv("PG_DB"),
			os.Getenv("PG_HOST"),
			os.Getenv("PG_PORT")),
	}
}
