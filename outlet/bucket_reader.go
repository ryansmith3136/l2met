package outlet

import (
	"fmt"
	"l2met/bucket"
	"l2met/store"
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
	for _ = range time.Tick(r.Interval) {
		buckets, err := r.Store.Scan()
		if err != nil {
			fmt.Printf("at=bucket.scan error=%s\n", err)
			continue
		}
		for bucket := range buckets {
			r.Inbox <- bucket
		}
	}
}

func (r *BucketReader) outlet() {
	for b := range r.Inbox {
		r.Store.Get(b)
		r.Outbox <- b
	}
}
