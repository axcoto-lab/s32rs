package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"os"
)

func initServer(r *mux.Router) {
	http.Handle("/", r)
	http.ListenAndServe(":3001", r)
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
