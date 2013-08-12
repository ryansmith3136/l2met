// The reader pkg is responsible for reading data from
// the store, building buckets from the data, and placing
// the buckets into a user-supplied channel.
package reader

import (
	"fmt"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/store"
	"time"
)

type Reader struct {
	str          store.Store
	scanInterval time.Duration
	numOutlets   int
	Inbox        chan *bucket.Bucket
	Outbox       chan *bucket.Bucket
	Mchan        *metchan.Channel
}

// Sets the scan interval to 1s.
func New(cfg *conf.D, st store.Store) *Reader {
	rdr := new(Reader)
	rdr.Inbox = make(chan *bucket.Bucket, cfg.BufferSize)
	rdr.numOutlets = cfg.Concurrency
	rdr.scanInterval = cfg.OutletInterval
	rdr.str = st
	return rdr
}

func (r *Reader) Start(out chan *bucket.Bucket) {
	r.Outbox = out
	go r.scan()
	for i := 0; i < r.numOutlets; i++ {
		go r.outlet()
	}
}

func (r *Reader) scan() {
	for _ = range time.Tick(r.scanInterval) {
		startScan := time.Now()
		buckets, err := r.str.Scan(r.str.Now())
		if err != nil {
			fmt.Printf("at=bucket.scan error=%s\n", err)
			continue
		}
		for b := range buckets {
			r.Inbox <- b
		}
		r.Mchan.Time("reader.scan", startScan)
	}
}

func (r *Reader) outlet() {
	for b := range r.Inbox {
		startGet := time.Now()
		r.str.Get(b)
		r.Outbox <- b
		r.Mchan.Time("reader.get", startGet)
	}
}
