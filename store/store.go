package store

import "l2met/bucket"

type Store interface {
	MaxPartitions() uint64
	Put(*bucket.Bucket) error
	Get(*bucket.Bucket) error
	Scan(string) <-chan *bucket.Bucket
	Health() bool
}
