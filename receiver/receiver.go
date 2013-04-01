package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"l2met/bucket"
	"l2met/store"
	"l2met/utils"
	"sync"
	"time"
)

// We read the body of an http request and then close the request.
// The processing of the body happens in a seperate routine. We use
// this struct to hold the data that is passed inbetween routines.
type LogRequest struct {
	// This is pulled from the password field of the HTTP request.
	Token string
	// The body of the HTTP request.
	Body []byte
	// Options from the query parameters
	Opts map[string][]string
}

type register struct {
	sync.Mutex
	m map[bucket.Id]*bucket.Bucket
}

type Receiver struct {
	// Keeping a register allows us to aggregate buckets in memory.
	// This decouples redis writes from HTTP requests.
	Register *register
	// After we pull data from the HTTP requests,
	// We put the data in the inbox to be processed.
	Inbox chan *LogRequest
	// The interval at which things are moved fron the inbox to the outbox
	TransferTicker *time.Ticker
	// After we flush our register of buckets, we put the
	// buckets in this channel to be flushed to redis.
	Outbox chan *bucket.Bucket
	// Flush buckets from register to redis. Number of seconds.
	FlushInterval time.Duration
	// Number of http request bodys to buffer.
	// These requests are waiting to go into the accept loop.
	MaxInbox  int
	MaxOutbox int
	// How many outlet routines should be running.
	NumOutlets int
	// How many accept routines should be running.
	NumAcceptors int
	// Bucket storage.
	Store store.Store
}

func NewReceiver() *Receiver {
	r := new(Receiver)
	r.Inbox = make(chan *LogRequest, r.MaxInbox)
	r.Outbox = make(chan *bucket.Bucket, r.MaxOutbox)
	r.Register = &register{m: make(map[bucket.Id]*bucket.Bucket)}
	return r
}

func (r *Receiver) Receive(token string, b []byte, opts map[string][]string) {
	r.Inbox <- &LogRequest{token, b, opts}
}

func (r *Receiver) Start(tick time.Duration) {
	// Parsing the log data can be expensive. Make use
	// of parallelism.
	for i := 0; i < r.NumAcceptors; i++ {
		go r.Accept()
	}
	// Each outlet will write a bucket to redis.
	for i := 0; i < r.NumOutlets; i++ {
		go r.Outlet()
	}
	r.TransferTicker = time.NewTicker(tick)
	// The transfer is not a concurrent process.
	// It removes buckets from the register to the outbox.
	go r.Transfer()
	//go r.report()
}

func (r *Receiver) Stop() {
	r.TransferTicker.Stop()
	// We sleep to give our transfer routine time to finish.
	time.Sleep(r.FlushInterval)
	close(r.Inbox)
	close(r.Outbox)
}

func (r *Receiver) Accept() {
	for lreq := range r.Inbox {
		rdr := bufio.NewReader(bytes.NewReader(lreq.Body))
		for bucket := range bucket.NewBucket(lreq.Token, rdr, lreq.Opts) {
			r.Register.Lock()
			k := *bucket.Id
			_, present := r.Register.m[k]
			if !present {
				r.Register.m[k] = bucket
			} else {
				r.Register.m[k].Add(bucket)
			}
			r.Register.Unlock()
		}
	}
}

func (r *Receiver) Transfer() {
	for _ = range r.TransferTicker.C {
		for k := range r.Register.m {
			r.Register.Lock()
			if m, ok := r.Register.m[k]; ok {
				delete(r.Register.m, k)
				r.Register.Unlock()
				r.Outbox <- m
			} else {
				r.Register.Unlock()
			}
		}
	}
}

func (r *Receiver) Outlet() {
	for b := range r.Outbox {
		err := r.Store.Put(b)
		if err != nil {
			fmt.Printf("error=%s\n", err)
		}
	}
}

// Keep an eye on the lenghts of our bufferes. If they are maxed out, something
// is going wrong.
func (r *Receiver) report() {
	for _ = range time.Tick(time.Second * 2) {
		utils.MeasureI("receiver.inbox", int64(len(r.Inbox)))
		utils.MeasureI("receiver.register", int64(len(r.Register.m)))
		utils.MeasureI("receiver.outbox", int64(len(r.Outbox)))
	}
}
