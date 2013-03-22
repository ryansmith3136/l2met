package store

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/bmizerany/pq"
	"l2met/encoding"
	"l2met/utils"
	"os"
	"time"
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

func WriteSliceToPostgres(batch []*Bucket, count int) int {
	defer utils.MeasureT("postgres.write.batch.time", time.Now())
	dropped := 0
	for pos := count - 1; pos >= 0; pos-- {
		bucket := batch[pos]
		err := WriteBucketToPostgres(bucket)
		if err != nil {
			utils.MeasureI("postgres.write.drop", 1)
			dropped++
		}
	}
	utils.MeasureI("postgres.write.attempted", int64(count))
	utils.MeasureI("postgres.write.dropped", int64(dropped))
	utils.MeasureI("postgres.write.success", int64(count-dropped))
	return (count - dropped)
}

func WriteBucketToPostgres(bucket *Bucket) error {
	defer utils.MeasureT("postgres.write.bucket.time", time.Now())
	if bucket == nil {
		utils.MeasureI("postgres.write.nilBucket.error", 1)
		return errors.New("got nil bucket")
	}
	tx, err := pg.Begin()
	if err != nil {
		utils.MeasureI("postgres.write.transactionBegin.error", 1)
		return err
	}
	if bucket.Vals == nil {
		err = bucket.GetFromRedis()
		if err != nil {
			utils.MeasureI("postgres.write.getBucket.error", 1)
			return err
		}
	}

	vals := string(encoding.EncodeArray(bucket.Vals, '{', '}', ','))

	row := tx.QueryRow(`
		SELECT id
		FROM buckets
		WHERE token = $1 AND measure = $2 AND source = $3 AND time = $4`,
		bucket.Key.Token, bucket.Key.Name, bucket.Key.Source, bucket.Key.Time)
	var id sql.NullInt64
	row.Scan(&id)

	if id.Valid {
		_, err = tx.Exec("UPDATE buckets SET vals = $1::FLOAT8[] WHERE id = $2",
			vals, id)
		if err != nil {
			tx.Rollback()
			utils.MeasureI("postgres.write.upsertBucket.error.count", 1)
			return err
		}
		utils.MeasureI("postgres.write.upsertBucket.success.count", 1)
	} else {
		_, err = tx.Exec(`
			INSERT INTO buckets(token, measure, source, time, vals)
			VALUES($1, $2, $3, $4, $5::FLOAT8[])`,
			bucket.Key.Token, bucket.Key.Name, bucket.Key.Source,
			bucket.Key.Time, vals)
		if err != nil {
			tx.Rollback()
			utils.MeasureI("postgres.write.newBucket.fail", 1)
			return err
		}
		utils.MeasureI("postgres.write.newBucket.success.count", 1)
	}

	err = tx.Commit()
	if err != nil {
		utils.MeasureI("postgres.write.transaction.close.error.count", 1)
		return err
	}
	return nil
}
