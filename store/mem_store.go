package store

import (
	"errors"
	"l2met/bucket"
	"sync"
)

type MemStore struct {
	sync.Mutex
	m map[bucket.Id]*bucket.Bucket
}

func NewMemStore() *MemStore {
	return &MemStore{m: make(map[bucket.Id]*bucket.Bucket)}
}

func (s *MemStore) Health() bool {
	return true
}

func (m *MemStore) MaxPartitions() uint64 {
	return uint64(1)
}

func (m *MemStore) Scan(partition string) <-chan *bucket.Bucket {
	m.Lock()
	buckets := make(chan *bucket.Bucket, 1000)
	go func(out chan *bucket.Bucket) {
		defer close(out)
		for _, bucket := range m.m {
			out <- bucket
		}
	}(buckets)
	return buckets
}

func (m *MemStore) Get(b *bucket.Bucket) error {
	m.Lock()
	bucket, present := m.m[*b.Id]
	if !present {
		return errors.New("Bucket not in MemStore.")
	}
	b = bucket
	m.Unlock()
	return nil
}

func (m *MemStore) Putback(partition string, id *bucket.Id) error {
	return nil
}

func (m *MemStore) Put(b *bucket.Bucket) error {
	m.Lock()
	m.m[*b.Id] = b
	m.Unlock()
	return nil
}
