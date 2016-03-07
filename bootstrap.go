package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
)

type App struct {
	Qe     *Queue
	DB     *DB
	R      *mux.Router
	Worker *Worker
}

func (a *App) init() {
	router := mux.NewRouter()

	router.Handle("/work", &WorkHandler{a})
	router.Handle("/job/{id}", &JobHandler{})
	a.R = router
}

func initWorker(a *App) {
	a.Worker = &Worker{5, a}
	go a.Worker.Work()
}

func initServer(a *App) {
	http.Handle("/", a.R)
	http.ListenAndServe(":3001", a.R)
}

func initQueue() *Queue {
	q := &Queue{
		Size: 100,
	}
	q.init()
	return q
}

func initDB() *DB {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		os.Getenv("PG_USER"),
		os.Getenv("PG_PWD"),
		os.Getenv("PG_DB"),
		os.Getenv("PG_HOST"),
		os.Getenv("PG_PORT"))

	log.Println(dbinfo)

	db := &DB{
		ConnString: dbinfo,
	}
	db.Connect()
	return db
}
