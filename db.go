package main

import (
	"database/sql"
	_ "github.com/lib/pq"
)

type DB struct {
	ConnString string
	db         *sql.DB
}

func (d *DB) Connect() error {
	db, err := sql.Open("postgres", d.ConnString)
	if err != nil {
		return err
	}
	d.db = db
	return nil
}

func (d *DB) Query(q string) (*sql.Rows, error) {
	return d.db.Query(q)
}

func (d *DB) Close() {
	d.db.Close()
}
