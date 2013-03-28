package token

import (
	"database/sql"
	"fmt"
	"github.com/bmizerany/pq"
	"os"
)

var (
	postgresEnabled = true
	pg              *sql.DB
	pgRead          *sql.DB
)

func init() {
	url := os.Getenv("DATABASE_URL")
	if len(url) == 0 {
		postgresEnabled = false
		fmt.Println("Postgres has been disabled.")
		return
	}
	str, err := pq.ParseURL(url)
	if err != nil {
		fmt.Printf("error=\"unable to parse DATABASE_URL\"\n")
		os.Exit(1)
	}
	pg, err = sql.Open("postgres", str)
	if err != nil {
		fmt.Printf("error=%s\n", err)
		os.Exit(1)
	}

	rurl := os.Getenv("DATABASE_READ_URL")
	if len(rurl) > 0 {
		rstr, err := pq.ParseURL(rurl)
		if err != nil {
			fmt.Printf("error=\"unable to parse DATABASE_READ_URL\"\n")
			os.Exit(1)
		}
		pgRead, err = sql.Open("postgres", rstr)
		if err != nil {
			fmt.Printf("error=%s\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("Missing DATABASE_READ_URL. Using DATABASE_URL to service reads.\n")
	pgRead, err = sql.Open("postgres", str)
	if err != nil {
		fmt.Printf("error=%s\n", err)
		os.Exit(1)
	}
}

func PingPostgres() error {
	_, err := pg.Query("select now()")
	return err
}
