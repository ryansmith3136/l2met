package main

import (
	"database/sql"
	"fmt"
	"l2met/encoding"
	"l2met/store"
	"l2met/utils"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

var (
	partitionId     uint64
	numPartitions   uint64
	workers         int
	processInterval int
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var tmp string
	var err error

	workers = 2
	tmp = os.Getenv("LOCAL_WORKERS")
	if len(tmp) != 0 {
		n, err := strconv.Atoi(tmp)
		if err == nil {
			workers = n
		}
	}

	tmp = os.Getenv("POSTGRES_INTERVAL")
	processInterval = 5
	if len(tmp) != 0 {
		n, err := strconv.Atoi(tmp)
		if err == nil {
			processInterval = n
		}
	}

	tmp = os.Getenv("NUM_OUTLET_PARTITIONS")
	numPartitions, err = strconv.ParseUint(tmp, 10, 64)
	if err != nil {
		log.Fatal("Unable to read NUM_OUTLET_PARTITIONS")
	}
}

func main() {
	var err error
	partitionId, err = utils.LockPartition(pg, "postgres_outlet", numPartitions)
	if err != nil {
		log.Fatal("Unable to lock partition.")
	}

	outbox := make(chan *store.Bucket, 1000)
	go scheduleFetch(outbox)
	for i := 0; i < workers; i++ {
		go handleBuckets(outbox)
	}

	// Print chanel metrics & live forever.
	report(outbox)
}

func report(o chan *store.Bucket) {
	for _ = range time.Tick(time.Second * 5) {
		utils.MeasureI("postgres_outlet.outbox", int64(len(o)))
	}
}

func scheduleFetch(outbox chan<- *store.Bucket) {
	for t := range time.Tick(time.Second) {
		if t.Second()%processInterval == 0 {
			fetch(t, outbox)
		}
	}
}

func fetch(t time.Time, outbox chan<- *store.Bucket) {
	fmt.Printf("at=start_fetch minute=%d\n", t.Minute())
	defer utils.MeasureT(time.Now(), "postgres_outlet.fetch")

	mailbox := fmt.Sprintf("postgres_outlet.%d", partitionId)
	for bucket := range store.ScanBuckets(mailbox) {
		outbox <- bucket
	}
}

func handleBuckets(outbox <-chan *store.Bucket) {
	for bucket := range outbox {
		err := writeToPostgres(bucket)
		if err != nil {
			log.Printf("measure=%q\n", "pg-write-failure")
			continue
		}
	}
}

func writeToPostgres(bucket *store.Bucket) error {
	tx, err := pg.Begin()
	if err != nil {
		return err
	}

	err = bucket.Get()
	if err != nil {
		return err
	}

	vals := string(encoding.EncodeArray(bucket.Vals))
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
			return err
		}
	} else {
		_, err = tx.Exec(`
			INSERT INTO buckets(token, measure, source, time, vals)
			VALUES($1, $2, $3, $4, $5::FLOAT8[])`,
			bucket.Key.Token, bucket.Key.Name, bucket.Key.Source,
			bucket.Key.Time, vals)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
