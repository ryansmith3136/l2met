package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"l2met/store"
	"l2met/utils"
	"log"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"time"
)

var libratoUrl = "https://metrics-api.librato.com/v1/metrics"

var (
	partitionId     uint64
	numPartitions   uint64
	workers         int
	processInterval int
	globalTokenUser string
	globalTokenPass string
	lockTTL         uint64
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	workers = utils.EnvInt("LOCAL_WORKERS", 2)
	processInterval = utils.EnvInt("LIBRATO_INTERVAL", 5)
	numPartitions = utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
	globalTokenUser = utils.EnvString("LIBRATO_USER", "")
	globalTokenPass = utils.EnvString("LIBRATO_TOKEN", "")
	lockTTL = utils.EnvUint64("LOCK_TTL", 10)

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

type LM struct {
	Name   string `json:"name"`
	Time   int64  `json:"measure_time"`
	Val    string `json:"value"`
	Source string `json:"source,omitempty"`
	Token  string `json:",omitempty"`
}

type LP struct {
	Gauges []*LM `json:"gauges"`
}

func main() {
	var err error
	partitionId, err = utils.LockPartition("librato_outlet", numPartitions, lockTTL)
	if err != nil {
		log.Fatal("Unable to lock partition.")
	}
	// The inbox is used to hold empty buckets that are
	// waiting to be processed. We buffer the chanel so
	// as not to slow down the fetch routine. We can
	inbox := make(chan *store.Bucket, 1000)
	// The converter will take items from the inbox,
	// fill in the bucket with the vals, then convert the
	// bucket into a librato metric.
	lms := make(chan *LM, 1000)
	// The converter will place the librato metrics into
	// the outbox for HTTP submission. We rely on the batch
	// routine to make sure that the collections of librato metrics
	// in the outbox are homogeneous with respect to their token.
	// This ensures that we route the metrics to the correct librato account.
	outbox := make(chan []*LM, 1000)

	// Routine that reads ints from the database
	// and sends them to the inbox.
	go scheduleFetch(inbox)

	// We take the empty buckets from the inbox,
	// get the values from the database, then make librato metrics out of them.
	for i := 0; i < workers; i++ {
		go scheduleConvert(inbox, lms)
	}

	// Shouldn't need to be concurrent since it's responsibility
	// it to serialize a collection of librato metrics.
	go batch(lms, outbox)

	// These routines involve reading data from the database
	// and making HTTP requests. We will want to take advantage of
	// parallel processing.
	for i := 0; i < workers; i++ {
		go post(outbox)
	}

	// Print chanel metrics & live forever.
	report(inbox, lms, outbox)
}

func report(i chan *store.Bucket, l chan *LM, o chan []*LM) {
	for _ = range time.Tick(time.Second * 5) {
		utils.MeasureI("librato_outlet.inbox", int64(len(i)))
		utils.MeasureI("librato_outlet.lms", int64(len(l)))
		utils.MeasureI("librato_outlet.outbox", int64(len(o)))
	}
}

// Fetch should kick off the librato outlet process.
// Its responsibility is to get the ids of buckets for the current time,
// make empty Buckets, then place the buckets in an inbox to be filled
// (load the vals into the bucket) and processed.
func scheduleFetch(inbox chan<- *store.Bucket) {
	for t := range time.Tick(time.Second) {
		// Start working on the new minute right away.
		if t.Second()%processInterval == 0 {
			fetch(t, inbox)
		}
	}
}

func fetch(t time.Time, inbox chan<- *store.Bucket) {
	fmt.Printf("at=start_fetch minute=%d\n", t.Minute())
	mailbox := fmt.Sprintf("librato_outlet.%d", partitionId)
	for bucket := range store.ScanBuckets(mailbox) {
		inbox <- bucket
	}
}

func scheduleConvert(inbox <-chan *store.Bucket, lms chan<- *LM) {
	for b := range inbox {
		convert(b, lms)
	}
}

func convert(b *store.Bucket, lms chan<- *LM) {
	err := b.Get()
	if err != nil {
		fmt.Printf("error=%s\n", err)
		return
	}
	if len(b.Vals) == 0 {
		fmt.Printf("at=bucket-no-vals bucket=%s\n", b.Key.Name)
		return
	}
	fmt.Printf("at=librato_outlet.process.bucket minute=%d name=%q\n",
		b.Key.Time.Minute(), b.Key.Name)
	k := b.Key
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".last", Val: ff(b.Last())}
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".min", Val: ff(b.Min())}
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".max", Val: ff(b.Max())}
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".mean", Val: ff(b.Mean())}
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".median", Val: ff(b.Median())}
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".perc95", Val: ff(b.P95())}
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".perc99", Val: ff(b.P99())}
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".count", Val: fi(b.Count())}
	lms <- &LM{Token: k.Token, Time: ft(k.Time), Source: k.Source, Name: k.Name + ".sum", Val: ff(b.Sum())}
}

func ff(x float64) string {
	return strconv.FormatFloat(x, 'f', 5, 64)
}

func fi(x int) string {
	return strconv.FormatInt(int64(x), 10)
}

func ft(t time.Time) int64 {
	return t.Unix() + 59
}

func batch(lms <-chan *LM, outbox chan<- []*LM) {
	ticker := time.Tick(time.Second)
	batchMap := make(map[string][]*LM)
	for {
		select {
		case <-ticker:
			for k, v := range batchMap {
				if len(v) > 0 {
					outbox <- v
				}
				delete(batchMap, k)
			}
		case lm := <-lms:
			_, present := batchMap[lm.Token]
			if !present {
				batchMap[lm.Token] = make([]*LM, 1, 50)
				batchMap[lm.Token][0] = lm
			} else {
				batchMap[lm.Token] = append(batchMap[lm.Token], lm)
			}
			if len(batchMap[lm.Token]) == cap(batchMap[lm.Token]) {
				outbox <- batchMap[lm.Token]
				delete(batchMap, lm.Token)
			}
		}
	}
}

func post(outbox <-chan []*LM) {
	for metrics := range outbox {
		if len(metrics) < 1 {
			fmt.Printf("at=%q\n", "empty-metrics-error")
			continue
		}

		sampleMetric := metrics[0]
		token := store.Token{Id: sampleMetric.Token}

		// If a global user/token is provided, use the token for all metrics.
		// This enable a databaseless librato_outlet.
		if len(globalTokenUser) == 0 || len(globalTokenPass) == 0 {
			token.Get()
		} else {
			token.User = globalTokenUser
			token.Pass = globalTokenPass
		}

		payload := new(LP)
		payload.Gauges = metrics

		j, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("at=json-marshal-error error=%s\n", err)
			continue
		}

		if len(j) == 0 {
			fmt.Printf("at=empty-body-error body=%s\n", j)
			continue
		}
		fmt.Printf("at=%q name=%s source=%s len=%d\n",
			"post-metric", sampleMetric.Name, sampleMetric.Source,
			len(metrics))
		maxRetry := 5
		for i := 0; i <= maxRetry; i++ {
			b := bytes.NewBuffer(j)
			req, err := http.NewRequest("POST", libratoUrl, b)
			if err != nil {
				fmt.Printf("at=%q error=%s body=%s\n", "request-error", err, b)
				continue
			}
			req.Header.Add("Content-Type", "application/json")
			req.SetBasicAuth(token.User, token.Pass)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("at=%q error=%s body=%s\n", "do-error", err, b)
				continue
			}
			if resp.StatusCode/100 == 2 {
				resp.Body.Close()
				utils.MeasureI("librato-http-post", 1)
				break
			} else {
				resp.Body.Close()
				if i == maxRetry {
					fmt.Printf("at=%q status=%d\n",
						"librato-status-error", resp.StatusCode)
				}
			}
		}
	}
}
