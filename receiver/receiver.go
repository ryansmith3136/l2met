// Receiver provides mechanisms to read log requests,
// extract measurements from log requests, aggregate
// measurements in buckets, and flush buckets into a memory store.
package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/parser"
	"github.com/ryandotsmith/l2met/store"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// We read the body of an http request and then close the request.
// The processing of the body happens in a seperate routine. We use
// this struct to hold the data that is passed inbetween routines.
type LogRequest struct {
	// The body of the HTTP request.
	Body []byte
	// Options from the query parameters
	Opts map[string][]string
}

// The register accumulates buckets in memory.
// A seperate routine working on an interval will flush
// the buckets from the register.
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
	// How many outlet routines should be running.
	NumOutlets int
	// Bucket storage.
	Store store.Store
	//Count the number of times we accept a bucket.
	numBuckets uint64
	// The number of time units allowed to pass before dropping a
	// log line.
	deadline int64
	// Publish receiver metrics on this channel.
	Mchan      *metchan.Channel
	outletters sync.WaitGroup
	acceptors  sync.WaitGroup
}

func NewReceiver(cfg *conf.D, s store.Store) *Receiver {
	r := new(Receiver)
	r.Inbox = make(chan *LogRequest, cfg.BufferSize)
	r.Outbox = make(chan *bucket.Bucket, cfg.BufferSize)
	r.Register = &register{m: make(map[bucket.Id]*bucket.Bucket)}
	r.FlushInterval = cfg.FlushInterval
	r.NumOutlets = cfg.Concurrency
	r.deadline = cfg.ReceiverDeadline
	r.numBuckets = uint64(0)
	r.Store = s
	return r
}

func (r *Receiver) Receive(b []byte, opts map[string][]string) {
	r.Inbox <- &LogRequest{b, opts}
}

// Start moving data through the receiver's pipeline.
func (r *Receiver) Start() {
	// Accepting the data involves parsing logs messages
	// into buckets. It is mostly CPU bound, so
	// it makes sense to parallelize this to the extent
	// of the number of CPUs.
	for i := 0; i < runtime.NumCPU(); i++ {
		go r.accept()
	}
	// Outletting data to the store involves sending
	// data out on the network to Redis. We may wish to
	// add more threads here since it is likely that
	// they will be blocking on I/O.
	for i := 0; i < r.NumOutlets; i++ {
		go r.outlet()
	}
	r.TransferTicker = time.NewTicker(r.FlushInterval)
	// The transfer is not a concurrent process.
	// It removes buckets from the register to the outbox.
	go r.transfer()
}

func (r *Receiver) Wait() {
	close(r.Inbox)
	r.acceptors.Wait()
	for {
		time.Sleep(time.Millisecond)
		if len(r.Register.m) == 0 {
			break
		}
	}
	close(r.Outbox)
	r.outletters.Wait()
}

func (r *Receiver) accept() {
	r.acceptors.Add(1)
	defer r.acceptors.Done()
	for lreq := range r.Inbox {
		rdr := bufio.NewReader(bytes.NewReader(lreq.Body))
		startParse := time.Now()
		for bucket := range parser.BuildBuckets(rdr, lreq.Opts) {
			if bucket.Id.Delay(r.Store.Now()) <= r.deadline {
				r.addRegister(bucket)
			} else {
				r.Mchan.Measure("receiver.drop", 1)
			}
		}
		r.Mchan.Time("receiver.accept", startParse)
	}
}

func (r *Receiver) addRegister(b *bucket.Bucket) {
	r.Register.Lock()
	defer r.Register.Unlock()
	k := *b.Id
	_, present := r.Register.m[k]
	if !present {
		r.Mchan.Measure("receiver.add-bucket", 1)
		r.Register.m[k] = b
	} else {
		r.Mchan.Measure("receiver.merge-bucket", 1)
		r.Register.m[k].Add(b)
	}
	r.numBuckets += 1
}

func (r *Receiver) transfer() {
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

func (r *Receiver) outlet() {
	r.outletters.Add(1)
	defer r.outletters.Done()
	for b := range r.Outbox {
		startPut := time.Now()
		if err := r.Store.Put(b); err != nil {
			fmt.Printf("error=%s\n", err)
		}
		r.Mchan.Time("reciever.outlet", startPut)
	}
}

// Keep an eye on the lenghts of our bufferes.
// If they are maxed out, something is going wrong.
func (r *Receiver) Report() {
	for _ = range time.Tick(time.Second * 2) {
		nb := atomic.LoadUint64(&r.numBuckets)
		atomic.AddUint64(&r.numBuckets, -nb)
		pre := "reciever.buffer."
		r.Mchan.Measure(pre+"buckets", float64(nb))
		r.Mchan.Measure(pre+"inbox", float64(len(r.Inbox)))
		r.Mchan.Measure(pre+"register", float64(len(r.Register.m)))
		r.Mchan.Measure(pre+"outbox", float64(len(r.Outbox)))
	}
}
