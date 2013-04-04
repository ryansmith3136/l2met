package outlet

import (
	"fmt"
	"l2met/bucket"
	"l2met/store"
	"l2met/utils"
	"time"
)

type BucketReader struct {
	Store       store.Store
	Interval    time.Duration
	Ttl         uint64
	NumOutlets  int
	NumScanners int
	Inbox       chan *bucket.Bucket
	Outbox      chan *bucket.Bucket
	Partition   string
}

func NewBucketReader(mi int) *BucketReader {
	rdr := new(BucketReader)
	rdr.Partition = "bucket-reader"
	rdr.Inbox = make(chan *bucket.Bucket, mi)
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
		//TODO(ryandotsmith): It is a shame that we have to lock
		//for each interval. It would be great if we could get a lock
		//and work for like 1,000 intervals and then relock.
		p, err := utils.LockPartition(r.Partition, r.Store.MaxPartitions(), r.Ttl)
		if err != nil {
			continue
		}
		partition := fmt.Sprintf("outlet.%d", p)
		for bucket := range r.Store.Scan(partition) {
			valid := time.Now().Add(bucket.Id.Resolution)
			//TODO(ryandotsmith): This seems ripe for a lua script.
			//The goal would to be receive data from scan that is sure
			//to be valid.
			if bucket.Id.Time.Before(valid) {
				r.Inbox <- bucket
			} else {
				if err := r.Store.Putback(partition, bucket.Id); err != nil {
					fmt.Printf("error=%s\n", err)
				}
			}
		}
		utils.UnlockPartition(fmt.Sprintf("%s.%d", r.Partition, p))
	}
}
func (r *BucketReader) outlet() {
	for b := range r.Inbox {
		r.Store.Get(b)
		r.Outbox <- b
	}
}
