package main

import (
	"database/sql"
	"github.com/bmizerany/pq"
	"log"
	"os"
)

var pg *sql.DB

func init() {
	url := os.Getenv("DATABASE_URL")
	if len(url) == 0 {
		log.Fatal("Must set DATABASE_URL.")
	}

	pgurl, err := pq.ParseURL(url)
	if err != nil {
		log.Fatal("Unable to parse DATABASE_URL.")
	}

	pg, err = sql.Open("postgres", pgurl)
	if err != nil {
		log.Fatal("Unable to open connection to PostgreSQL.")
	}

	pg.Exec("set application_name = 'l2met-next_postgres'")
}
