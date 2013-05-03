package store

import "l2met/bucket"

type Store interface {
	MaxPartitions() uint64
	Put(*bucket.Bucket) error
	Putback(string, *bucket.Id) error
	Get(*bucket.Bucket) error
	Scan() (<-chan *bucket.Bucket, error)
	Health() bool
}
