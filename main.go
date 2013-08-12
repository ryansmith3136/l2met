// L2met converts a formatted log stream into metrics.
package main

import (
	"flag"
	"fmt"
	"github.com/ryandotsmith/l2met/auth"
	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/outlet"
	"github.com/ryandotsmith/l2met/reader"
	"github.com/ryandotsmith/l2met/receiver"
	"github.com/ryandotsmith/l2met/store"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// Hold onto the app's global config.
var cfg *conf.D

func main() {
	cfg = conf.New()
	flag.Parse()

	// Can be passed to other modules
	// as an internal metrics channel.
	mchan := metchan.New(cfg.Verbose, cfg.MetchanUrl)
	mchan.Start()

	// The store will be used by receivers and outlets.
	var st store.Store
	if len(cfg.RedisHost) > 0 {
		redisStore := store.NewRedisStore(cfg)
		redisStore.Mchan = mchan
		st = redisStore
		fmt.Printf("at=initialized-redis-store\n")
	} else {
		st = store.NewMemStore()
		fmt.Printf("at=initialized-mem-store\n")
	}

	if cfg.UseOutlet {
		rdr := reader.New(cfg, st)
		rdr.Mchan = mchan
		outlet := outlet.NewLibratoOutlet(cfg, rdr)
		outlet.Mchan = mchan
		outlet.Start()
	}

	if cfg.UsingReciever {
		recv := receiver.NewReceiver(cfg, st)
		recv.Mchan = mchan
		recv.Start()
		if cfg.Verbose {
			go recv.Report()
		}
		http.HandleFunc("/logs",
			func(w http.ResponseWriter, r *http.Request) {
				startReceiveT := time.Now()
				if r.Method != "POST" {
					http.Error(w, "Invalid Request", 400)
					return
				}
				// If we can decrypt the authentication
				// we know it is valid and thus good enought
				// for our receiver. Later, another routine
				// can extract the username and password from
				// the auth to use it against the Librato API.
				authLine, ok := r.Header["Authorization"]
				if !ok && len(authLine) > 0 {
					http.Error(w, "Missing Auth.", 400)
					return
				}
				parseRes, err := auth.Parse(authLine[0])
				if err != nil {
					http.Error(w, "Fail: Parse auth.", 400)
					return
				}
				_, _, err = auth.Decrypt(parseRes)
				if err != nil {
					http.Error(w, "Invalid Request", 400)
					return
				}
				v := r.URL.Query()
				v.Add("auth", parseRes)

				b, err := ioutil.ReadAll(r.Body)
				r.Body.Close()
				if err != nil {
					http.Error(w, "Invalid Request", 400)
					return
				}
				recv.Receive(b, v)
				mchan.Time("http.accept", startReceiveT)
			})
	}

	// The only thing that constitutes a healthy l2met
	// is the health of the store. In some cases, this might mean
	// a Redis health check.
	http.HandleFunc("/health",
		func(w http.ResponseWriter, r *http.Request) {
			ok := st.Health()
			if !ok {
				msg := "Store is unavailable."
				fmt.Printf("error=%q\n", msg)
				http.Error(w, msg, 500)
			}
		})

	http.HandleFunc("/sign",
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				http.Error(w, "Method must be POST.", 400)
				return
			}
			l := r.Header.Get("Authorization")
			user, err := auth.Parse(l)
			if err != nil {
				http.Error(w, "Unable to parse headers.", 400)
				return
			}
			matched := false
			for i := range cfg.Secrets {
				if user == cfg.Secrets[i] {
					matched = true
					break
				}
			}
			if !matched {
				http.Error(w, "Authentication failed.", 401)
				return
			}
			b, err := ioutil.ReadAll(r.Body)
			r.Body.Close()
			if err != nil {
				http.Error(w, "Unable to read body.", 400)
				return
			}
			signed, err := auth.EncryptAndSign(b)
			if err != nil {
				http.Error(w, "Unable to sign body.", 500)
				return
			}
			fmt.Fprint(w, string(signed))
		})

	// Start the HTTP server.
	e := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), nil)
	if e != nil {
		log.Fatal("Unable to start HTTP server.")
	}
	fmt.Printf("at=l2met-initialized port=%d\n", cfg.Port)
}
