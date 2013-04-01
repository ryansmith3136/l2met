package main

import (
	"l2met/outlet"
	"l2met/store"
	"l2met/utils"
	"log"
	"net"
	"net/http"
	"runtime"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// Set timeouts on all http connections.
// We don't want to wait forever on librato.
func init() {
	http.DefaultTransport = &http.Transport{
		DisableKeepAlives: true,
		Dial: func(n, a string) (net.Conn, error) {
			c, err := net.DialTimeout(n, a, time.Second*5)
			if err != nil {
				return c, err
			}
			return c, c.SetDeadline(time.Now().Add(time.Second * 7))
		},
	}
}

func main() {
	concurrency := utils.EnvInt("LOCAL_WORKERS", 2)
	numPartitions := utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
	server, pass, err := utils.ParseRedisUrl()
	if err != nil {
		log.Fatal(err)
	}
	rs := store.NewRedisStore(server, pass, numPartitions, concurrency*2)

	rdr := outlet.NewBucketReader()
	rdr.NumScanners = concurrency
	rdr.NumOutlets = concurrency
	rdr.Store = rs
	rdr.Interval = time.Millisecond * 500

	l := outlet.NewLibratoOutlet()
	l.Reader = rdr
	l.NumOutlets = concurrency
	l.NumConverters = concurrency
	l.Retries = 2
	l.User = utils.EnvString("LIBRATO_USER", "")
	l.Pass = utils.EnvString("LIBRATO_TOKEN", "")
	l.Start()
	select {}
}
