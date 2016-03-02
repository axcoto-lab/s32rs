package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
)

type AwsKey struct {
	Key    string
	Secret string
}

func main() {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable", os.Getenv("PG_USER"), os.Getenv("PG_PWD"), os.Getenv("PG_DB"), os.Getenv("PG_HOST"), os.Getenv("PG_PORT"))

	loadS3(dbinfo, os.Getenv("AWS_BUCKET_S32RS"), "manifest.json", &AwsKey{os.Getenv("AWS_KEY"), os.Getenv("AWS_SECRET")})
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func loadS3(dbinfo, bucket, manifest string, aws *AwsKey) {
	log.Printf("DBInfo %s\n", dbinfo)

	db, err := sql.Open("postgres", dbinfo)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Start copy")
	rows, err := db.Query(fmt.Sprintf(`COPY vinh_test1
	FROM 's3://%s/%s'
	credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
	CSV
	IGNOREHEADER 1`, bucket, manifest, aws.Key, aws.Secret))
	defer rows.Close()
	log.Println("Done copy")

	if rows != nil {
		for rows.Next() {
			var count int
			err = rows.Scan(&count)

			log.Printf("Loaded %f", count)
		}
	}
}
