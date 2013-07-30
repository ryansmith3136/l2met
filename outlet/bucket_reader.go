package outlet

import (
	"fmt"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/store"
	"time"
)

type BucketReader struct {
	Store       store.Store
	Interval    time.Duration
	Partition   string
	Ttl         uint64
	NumOutlets  int
	NumScanners int
	Inbox       chan *bucket.Bucket
	Outbox      chan *bucket.Bucket
	Mchan       *metchan.Channel
}

func NewBucketReader(sz, c int, i time.Duration, st store.Store) *BucketReader {
	rdr := new(BucketReader)
	rdr.Partition = "bucket-reader"
	rdr.Inbox = make(chan *bucket.Bucket, sz)
	rdr.NumScanners = c
	rdr.NumOutlets = c
	rdr.Interval = i
	rdr.Store = st
	return rdr
}

func (r *BucketReader) Start(out chan *bucket.Bucket) {
	r.Outbox = out
	go r.scan()
	for i := 0; i < r.NumOutlets; i++ {
		go r.outlet()
	}
}

func (r *BucketReader) scan() {
	for t := range time.Tick(r.Interval) {
		startScan := time.Now()
		buckets, err := r.Store.Scan(t)
		if err != nil {
			fmt.Printf("at=bucket.scan error=%s\n", err)
			continue
		}
		for b := range buckets {
			r.Inbox <- b
		}
		r.Mchan.Measure("reader.scan", startScan)
	}
}

func (r *BucketReader) outlet() {
	for b := range r.Inbox {
		startGet := time.Now()
		r.Store.Get(b)
		r.Outbox <- b
		r.Mchan.Measure("reader.get", startGet)
	}
}
