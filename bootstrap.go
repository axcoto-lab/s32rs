package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/kabukky/httpscerts"
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

func initCert() {
	err := httpscerts.Check("cert.pem", "key.pem")
	if err != nil {
		err = httpscerts.Generate("cert.pem", "key.pem", "0.0.0.0:3002")
		if err != nil {
			log.Fatal("Error: Couldn't create https certs.")
		}
	}
}

func initServer(a *App) {
	http.Handle("/", a.R)
	go func() {
		http.ListenAndServe(":3001", a.R)
	}()

	http.ListenAndServeTLS(":3002", "cert.pem", "key.pem", a.R)
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
