package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"l2met/store"
	"l2met/utils"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"sync"
	"time"
  "strconv"
)

var (
	metricsPat     = regexp.MustCompile(`\/metrics\/(.*)\??`)
	workers        int
	port           string
	registerLocker sync.Mutex
	numPartitions  uint64
	reqBuffer      int
	flushInterval  int
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	port = utils.EnvString("PORT", "8000")
	workers = utils.EnvInt("LOCAL_WORKERS", 2)
	reqBuffer = utils.EnvInt("REQUEST_BUFFER", 1000)
	flushInterval = utils.EnvInt("FLUSH_INTERVAL", 1)
	numPartitions = utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
}

type LogRequest struct {
	Token string
	Body  []byte
}

func main() {
	fmt.Printf("at=start-l2met port=%s\n", port)
	register := make(map[store.BKey]*store.Bucket)
	inbox := make(chan *LogRequest, reqBuffer)
	outbox := make(chan *store.Bucket, reqBuffer)

	go report(inbox, outbox, register)
	for i := 0; i < workers; i++ {
		go accept(inbox, register)
	}
	go transfer(register, outbox)
	for i := 0; i < workers; i++ {
		go outlet(outbox)
	}

	receiver := func(w http.ResponseWriter, r *http.Request) { receiveLogs(w, r, inbox) }
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {})
	http.HandleFunc("/logs", receiver)
	http.HandleFunc("/metrics/", getMetrics)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Printf("at=error error=\"Unable to start http server.\"\n")
		os.Exit(1)
	}
}

func report(inbox chan *LogRequest, outbox chan *store.Bucket, register map[store.BKey]*store.Bucket) {
	for _ = range time.Tick(time.Second * 2) {
		utils.MeasureI("web.inbox", int64(len(inbox)))
		utils.MeasureI("web.register", int64(len(register)))
		utils.MeasureI("web.outbox", int64(len(outbox)))
	}
}

func receiveLogs(w http.ResponseWriter, r *http.Request, inbox chan<- *LogRequest) {
	defer utils.MeasureT("l2met-kernel-production.http-receiver", time.Now())
	if r.Method != "POST" {
		http.Error(w, "Invalid Request", 400)
		return
	}
	token, err := utils.ParseToken(r)
	if err != nil {
		http.Error(w, "Invalid Request", 400)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid Request", 400)
		r.Body.Close()
		return
	}
	r.Body.Close()
	inbox <- &LogRequest{token, b}
}

func accept(inbox <-chan *LogRequest, register map[store.BKey]*store.Bucket) {
	for lreq := range inbox {
		rdr := bufio.NewReader(bytes.NewReader(lreq.Body))
		for bucket := range store.NewBucket(lreq.Token, rdr) {
			registerLocker.Lock()
			k := bucket.Key
			_, present := register[k]
			if !present {
				fmt.Printf("at=%q minute=%d name=%s\n",
					"add-to-register", bucket.Key.Time.Minute(), bucket.Key.Name)
				register[k] = bucket
			} else {
				register[k].Add(bucket)
			}
			registerLocker.Unlock()
		}
	}
}

func transfer(register map[store.BKey]*store.Bucket, outbox chan<- *store.Bucket) {
	for _ = range time.Tick(time.Second * time.Duration(flushInterval)) {
		for k := range register {
			registerLocker.Lock()
			if m, ok := register[k]; ok {
				delete(register, k)
				registerLocker.Unlock()
				outbox <- m
			} else {
				registerLocker.Unlock()
			}
		}
	}
}

func outlet(outbox <-chan *store.Bucket) {
	for b := range outbox {
		err := b.Put(numPartitions)
		if err != nil {
			fmt.Printf("error=%s\n", err)
		}
	}
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
  defer utils.MeasureT("http-metrics", time.Now())

	// Support CORS.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

	names := metricsPat.FindStringSubmatch(r.URL.Path)
	if len(names) < 2 {
		fmt.Printf("at=error error=%q\n", "Name parameter not provided.")
		errmsg := map[string]string{"error": "Name parameter not provided."}
		utils.WriteJson(w, 401, errmsg)
		return
	}
	name := names[1]

	token, err := utils.ParseToken(r)
	if err != nil {
		fmt.Printf("at=error error=%q\n", err)
		errmsg := map[string]string{"error": "Missing authorization."}
		utils.WriteJson(w, 401, errmsg)
		return
	}

	q := r.URL.Query()
	limit, err := strconv.ParseInt(q.Get("limit"), 10, 32)
	if err != nil {
		errmsg := map[string]string{"error": "Missing limit parameter."}
		utils.WriteJson(w, 400, errmsg)
		return
	}

	resolution, err := strconv.ParseInt(q.Get("resolution"), 10, 32)
	if err != nil {
		errmsg := map[string]string{"error": "Missing resolution parameter."}
		utils.WriteJson(w, 400, errmsg)
		return
	}

	max := utils.RoundTime(time.Now(), (time.Minute * time.Duration(resolution)))
	min := max.Add(-1 * time.Minute * time.Duration(limit*resolution))

	metrics, err := store.GetMetrics(token, name, resolution, min, max)
	if err != nil {
		errmsg := map[string]string{"error": "Unable to find metrics."}
		utils.WriteJson(w, 500, errmsg)
		return
	}
	utils.WriteJson(w, 200, metrics)
}
