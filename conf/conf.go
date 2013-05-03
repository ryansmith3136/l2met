package conf

import (
	"flag"
	"l2met/utils"
	"os"
	"time"
)

var Outlet string

func init() {
	flag.StringVar(&Outlet, "outlet", "", "Type of outlet to start. Valid opts: librato, graphite, (blank).")
}

var (
	BufferSize     int
	Concurrency    int
	FlushtInterval time.Duration
)

func init() {
	b := utils.EnvInt("REQUEST_BUFFER", 1000)
	flag.IntVar(&BufferSize, "buffer", b, "Number of items to buffer prior to flush.")

	c := utils.EnvInt("CONCURRENCY", 100)
	flag.IntVar(&Concurrency, "concurrency", c, "Number local routines to start.")

	t := time.Millisecond * 200
	flag.DurationVar(&FlushtInterval, "flush-interval", t, "Time to wait before flushing items in buffer.")
}

var (
	UsingRedis    bool
	MaxRedisConns int
	RedisHost     string
	RedisPass     string
	MaxPartitions uint64
)

func init() {
	var err error

	rurl := os.Getenv("REDIS_URL")
	if len(rurl) == 0 {
		UsingRedis = false
		return
	}

	RedisHost, RedisPass, err = utils.ParseRedisUrl(rurl)
	if err != nil {
		UsingRedis = false
		return
	}

	mr := Concurrency + 10
	flag.IntVar(&MaxRedisConns, "max-redis-conns", mr, "Max number of redis connections to pool.")

	flag.Uint64Var(&MaxPartitions, "max-partitions", uint64(1), "Max number of partitions.")
}

var UsingHttpOutlet bool

func init() {
	flag.BoolVar(&UsingHttpOutlet, "http-outlet", true, "Enable HTTP Outlet.")
}

var NumOutletRetry int

func init() {
	flag.IntVar(&NumOutletRetry, "outlet-retry", 2, "Number of times to retry outlet requests.")
}

var UsingReciever bool

func init() {
	flag.BoolVar(&UsingReciever, "receiver", true, "Start a log receiver.")
}

var (
	Verbose bool
	Port    int
)

func init() {
	flag.BoolVar(&Verbose, "v", false, "Enable verbose stastics.")

	p := utils.EnvInt("PORT", 8080)
	flag.IntVar(&Port, "port", p, "HTTP server will bind to this port.")
}

//Finally.
func init() {
	flag.Parse()
}
