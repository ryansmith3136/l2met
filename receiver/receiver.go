// Receiver provides mechanisms to read log requests,
// extract measurements from log requests, aggregate
// measurements in buckets, and flush buckets into a memory store.
package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/store"
	"github.com/ryandotsmith/l2met/utils"
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
	// Publish receiver metrics on this channel.
	Mchan *metchan.Channel
}

func NewReceiver(sz, c int, i time.Duration, s store.Store) *Receiver {
	r := new(Receiver)
	r.Inbox = make(chan *LogRequest, sz)
	r.Outbox = make(chan *bucket.Bucket, sz)
	r.Register = &register{m: make(map[bucket.Id]*bucket.Bucket)}
	r.numBuckets = uint64(0)
	r.FlushInterval = i
	r.NumOutlets = c
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

func (r *Receiver) Stop() {
	r.TransferTicker.Stop()
	// We sleep to give our transfer routine time to finish.
	time.Sleep(r.FlushInterval)
	close(r.Inbox)
	close(r.Outbox)
}

func (r *Receiver) accept() {
	for lreq := range r.Inbox {
		rdr := bufio.NewReader(bytes.NewReader(lreq.Body))
		startParse := time.Now()
		for bucket := range bucket.NewBuckets(rdr, lreq.Opts) {
			now := time.Now().Truncate(bucket.Id.Resolution)
			if bucket.Id.Time.Equal(now) {
				r.addRegister(bucket)
			} else {
				fmt.Printf("at=receiver-drop b=%s n=%s\n",
					bucket.Id.Time, now)
			}
		}
		r.Mchan.Measure("receiver.accept", startParse)
	}
}

func (r *Receiver) addRegister(b *bucket.Bucket) {
	r.Register.Lock()
	defer r.Register.Unlock()
	k := *b.Id
	_, present := r.Register.m[k]
	if !present {
		r.Register.m[k] = b
	} else {
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
	for b := range r.Outbox {
		startPut := time.Now()
		if err := r.Store.Put(b); err != nil {
			fmt.Printf("error=%s\n", err)
		}
		r.Mchan.Measure("reciever.outlet", startPut)
	}
}

// Keep an eye on the lenghts of our bufferes.
// If they are maxed out, something is going wrong.
func (r *Receiver) Report() {
	for _ = range time.Tick(time.Second * 2) {
		nb := atomic.LoadUint64(&r.numBuckets)
		atomic.AddUint64(&r.numBuckets, -nb)
		utils.MeasureI("receiver.buckets", int(nb))
		utils.MeasureI("receiver.inbox", len(r.Inbox))
		utils.MeasureI("receiver.register", len(r.Register.m))
		utils.MeasureI("receiver.outbox", len(r.Outbox))
	}
}
