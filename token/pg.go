package token

import (
	"database/sql"
	"fmt"
	"github.com/bmizerany/pq"
	"os"
)

var pg *sql.DB

func init() {
	url := os.Getenv("DATABASE_URL")
	if len(url) == 0 {
		fmt.Println("Postgres has been disabled.")
		return
	}

	str, err := pq.ParseURL(url)
	if err != nil {
		fmt.Printf("error=%q\n", "unable to parse DATABASE_URL")
		os.Exit(1)
	}
	pg, err = sql.Open("postgres", str)
	if err != nil {
		fmt.Printf("error=%s\n", err)
		os.Exit(1)
	}
}

func PingPostgres() error {
	_, err := pg.Query("select now()")
	return err
}
