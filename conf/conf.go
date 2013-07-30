// Conf exposes a data structure containing all of the
// l2met configuration data. Combines cmd flags and env vars.
package conf

import (
	"errors"
	"flag"
	"net/url"
	"os"
	"strings"
	"time"
)

type D struct {
	AppName         string
	Outlet          string
	RedisHost       string
	RedisPass       string
	MetchanUrl      *url.URL
	Secrets         []string
	BufferSize      int
	Concurrency     int
	Port            int
	NumOutletRetry  int
	MaxPartitions   uint64
	FlushtInterval  time.Duration
	UsingHttpOutlet bool
	UsingReciever   bool
	Verbose         bool
}

// Builds a conf data structure and connects
// the fields in the struct to flags.
// It is up to the caller to call flag.Parse()
func New() *D {
	d := new(D)

	flag.StringVar(&d.AppName, "app-name", "l2met",
		"Prefix internal log messages with this value.")

	flag.StringVar(&d.Outlet, "outlet", "",
		"The type of outlet to use."+
			"Example:librato, graphite, (blank)")

	flag.IntVar(&d.BufferSize, "buffer", 1024,
		"Max number of items for all internal buffers.")

	flag.IntVar(&d.Concurrency, "concurrency", 100,
		"Number of running go routines for outlet or receiver.")

	flag.IntVar(&d.Port, "port", 8080,
		"HTTP server's bind port.")

	flag.IntVar(&d.NumOutletRetry, "outlet-retry", 2,
		"Number of attempts to outlet metrics to Librato or Graphite.")

	flag.Uint64Var(&d.MaxPartitions, "partitions", uint64(1),
		"Number of partitions to use for outlets.")

	flag.DurationVar(&d.FlushtInterval, "flush-interval", time.Second,
		"Time to wait before sending data to store or outlet. "+
			"Example:60s 30s 1m")

	flag.BoolVar(&d.UsingHttpOutlet, "http-outlet", false,
		"Enable the HTTP outlet.")

	flag.BoolVar(&d.UsingReciever, "receiver", true,
		"Enable the Receiver.")

	flag.BoolVar(&d.Verbose, "v", false,
		"Enable verbose log output.")

	d.RedisHost, d.RedisPass, _ = parseRedisUrl(env("REDIS_URL"))
	d.Secrets = strings.Split(mustenv("SECRETS"), ":")

	if len(env("METCHAN_URL")) > 0 {
		url, err := url.Parse(env("METCHAN_URL"))
		if err == nil {
			d.MetchanUrl = url
		}
	}

	return d
}

// Helper Function
func env(n string) string {
	return os.Getenv(n)
}

func mustenv(n string) string {
	v := env(n)
	if len(v) == 0 {
		panic("Must set: " + n)
	}
	return v
}

// Helper Function
func parseRedisUrl(s string) (string, string, error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", errors.New("Missing REDIS_URL")
	}
	var password string
	if u.User != nil {
		password, _ = u.User.Password()
	}
	return u.Host, password, nil
}
