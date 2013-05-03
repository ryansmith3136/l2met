package main

import (
	"fmt"
	"io/ioutil"
	"l2met/conf"
	"l2met/outlet"
	"l2met/receiver"
	"l2met/store"
	"l2met/utils"
	"log"
	"net/http"
	"runtime"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	//The store will be used by receivers and outlets.
	var st store.Store
	if conf.UsingRedis {
		st = store.NewRedisStore(conf.RedisHost,
			conf.RedisPass, conf.MaxPartitions, conf.MaxRedisConns)
		fmt.Printf("at=initialized-redis-store\n")
	} else {
		st = store.NewMemStore()
		fmt.Printf("at=initialized-mem-store\n")
	}

	//It is not possible to run both librato and graphite outlets
	//in the same process.
	switch conf.Outlet {
	case "librato":
		rdr := outlet.NewBucketReader(conf.BufferSize,
			conf.Concurrency, conf.FlushtInterval, st)
		outlet := outlet.NewLibratoOutlet(conf.BufferSize,
			conf.Concurrency, conf.NumOutletRetry, rdr)
		outlet.Start()
		if conf.Verbose {
			go outlet.Report()
		}
	case "graphite":
		rdr := &outlet.BucketReader{Store: st, Interval: conf.FlushtInterval}
		outlet := outlet.NewGraphiteOutlet(conf.BufferSize, rdr)
		outlet.Start()
	default:
		fmt.Println("No outlet running. Run `l2met -h` for outlet help.")
	}

	//The HTTP Outlet can be ran in addition to the librato or graphite outlet.
	if conf.UsingHttpOutlet {
		httpOutlet := new(outlet.HttpOutlet)
		httpOutlet.Store = st
		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			httpOutlet.ServeReadBucket(w, r)
		})
	}

	if conf.UsingReciever {
		recv := receiver.NewReceiver(conf.BufferSize,
			conf.Concurrency, conf.FlushtInterval, st)
		recv.Start()
		if conf.Verbose {
			go recv.Report()
		}
		http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
			startReceiveT := time.Now()
			if r.Method != "POST" {
				http.Error(w, "Invalid Request", 400)
				return
			}
			user, pass, err := utils.ParseAuth(r)
			if err != nil {
				fmt.Printf("measure.failed-auth erro=%s user=%s pass=%s user-agent=%s token=%s client=%s\n",
					err, user, pass, r.Header.Get("User-Agent"), r.Header.Get("Logplex-Drain-Token"), r.Header.Get("X-Forwarded-For"))
				http.Error(w, "Invalid Request", 400)
				return
			}
			b, err := ioutil.ReadAll(r.Body)
			r.Body.Close()
			if err != nil {
				http.Error(w, "Invalid Request", 400)
				return
			}
			v := r.URL.Query()
			v.Add("user", user)
			v.Add("password", pass)
			recv.Receive(b, v)
			utils.MeasureT("http-receiver", startReceiveT)
		})
	}

	//The only thing that constitutes a healthy l2met
	//is the health of the store. In some cases, this might mean
	//a redis health check.
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ok := st.Health()
		if !ok {
			msg := "Store is unavailable."
			fmt.Printf("error=%q\n", msg)
			http.Error(w, msg, 500)
		}
	})

	http.HandleFunc("/sign", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Invalid Method. Must be POST.", 400)
			return
		}
		b, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			http.Error(w, "Unable to read body of POST.", 400)
			return
		}
		signed, err := utils.Sign(b)
		if err != nil {
			http.Error(w, "Invalid Request", 500)
			return
		}
		fmt.Fprint(w, string(signed))
	})

	//Start the HTTP server.
	if e := http.ListenAndServe(fmt.Sprintf(":%d", conf.Port), nil); e != nil {
		log.Fatal("Unable to start HTTP server.")
	}
	fmt.Printf("at=l2met-initialized port=%d\n", conf.Port)
}
