package outlet

import (
	"l2met/bucket"
	"l2met/store"
	"l2met/utils"
	"time"
	"fmt"
)

type BucketReader struct {
	Store     store.Store
	Interval  time.Duration
	Partition string
	Ttl       uint64
}

func (r *BucketReader) Start(out chan<- *bucket.Bucket) {
	for _ = range time.Tick(r.Interval) {
		p, err := utils.LockPartition(r.Partition, r.Store.MaxPartitions(), r.Ttl)
		if err != nil {
			continue
		}
		partition := fmt.Sprintf("outlet.%d", p)
		for bucket := range r.Store.Scan(partition) {
			r.Store.Get(bucket)
			out <- bucket
		}
	}
}
