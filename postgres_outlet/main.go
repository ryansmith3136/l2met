package main

import (
	"l2met/piping"
	"l2met/store"
	"l2met/utils"
	"log"
	"runtime"
	"time"
)

var (
	partitionId     uint64
	numPartitions   uint64
	workers         int
	processInterval int
	lockTTL         uint64
	database_url    string
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	workers = utils.EnvInt("LOCAL_WORKERS", 2)
	processInterval = utils.EnvInt("POSTGRES_INTERVAL", 5)
	numPartitions = utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
	lockTTL = utils.EnvUint64("LOCK_TTL", 10)
}

func main() {
	var err error
	if err != nil {
		log.Fatal("Unable to lock partition.")
	}
	redisSource := piping.NewRedisSource(1, numPartitions, lockTTL, "postgres_outlet")
	redisSource.Start()
	outlets := make([]*piping.PostgresOutlet, workers)
	redisOutbox := redisSource.GetOutput()
	for i := 0; i < workers; i++ {
		outlets[i] = piping.NewPostgresOutlet(redisOutbox, 2000, 60)
		outlets[i].Start()
	}

	// Print chanel metrics & live forever.
	report(redisOutbox)
}

func report(o chan *store.Bucket) {
	for _ = range time.Tick(time.Second * 5) {
		utils.MeasureI("postgres_outlet.outbox", int64(len(o)))
	}
}
