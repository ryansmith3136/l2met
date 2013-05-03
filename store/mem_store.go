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

func (m *MemStore) Scan() (<-chan *bucket.Bucket, error) {
	m.Lock()
	//TODO(ryandotsmith): Can we eliminate the magical number?
	buckets := make(chan *bucket.Bucket, 1000)
	go func(out chan *bucket.Bucket) {
		defer m.Unlock()
		defer close(out)
		for k, v := range m.m {
			delete(m.m, k)
			out <- v
		}
	}(buckets)
	return buckets, nil
}

func (m *MemStore) Get(b *bucket.Bucket) error {
	m.Lock()
	defer m.Unlock()
	bucket, present := m.m[*b.Id]
	if !present {
		return errors.New("Bucket not in MemStore.")
	}
	b = bucket
	return nil
}

func (m *MemStore) Putback(partition string, id *bucket.Id) error {
	return nil
}

func (m *MemStore) Put(b *bucket.Bucket) error {
	m.Lock()
	defer m.Unlock()
	m.m[*b.Id] = b
	return nil
}
