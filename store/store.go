// The store pkg is responsible for coordinating bucket transfer
// between the receivers (front-end) and readers & outlets (back-end).
package store

import (
	"github.com/ryandotsmith/l2met/bucket"
	"time"
)

type Store interface {
	MaxPartitions() uint64
	Put(*bucket.Bucket) error
	Get(*bucket.Bucket) error
	Scan(time.Time) (<-chan *bucket.Bucket, error)
	Now() time.Time
	Health() bool
}
