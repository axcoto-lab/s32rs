package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
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

func (d *DB) CreateBillTable(table string) {
	schemaQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s  (
		invoiceid character varying(256),
		payeraccountid character varying(256),
		linkedaccountid character varying(256),
		recordtype character varying(256),
		recordid character varying(256) PRIMARY KEY,
		productname character varying(256),
		rateid character varying(256),
		subscriptionid character varying(256),
		pricingplanid character varying(256),
		usagetype character varying(256),
		operation character varying(256),
		availabilityzone character varying(256),
		reservedinstance character varying(256),
		itemdescription character varying(256),
		usagestartdate character varying(256),
		usageenddate character varying(256),
		usagequantity numeric(38, 16) NULL,
		rate numeric(38, 16) NULL,
		cost numeric(38, 16) NULL,
		resourceid character varying(256),
		"user:cluster" char(1))`, table)
	log.Printf("Schema: %s", schemaQuery)
	d.db.Query(schemaQuery)
}
