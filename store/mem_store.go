package store

import (
	"errors"
	"github.com/ryandotsmith/l2met/bucket"
	"sync"
	"time"
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

func (m *MemStore) Now() time.Time {
	return time.Now()
}

func (m *MemStore) Scan(schedule time.Time) (<-chan *bucket.Bucket, error) {
	m.Lock()
	//TODO(ryandotsmith): Can we eliminate the magical number?
	buckets := make(chan *bucket.Bucket, 1000)
	go func(out chan *bucket.Bucket) {
		defer m.Unlock()
		defer close(out)
		for k, v := range m.m {
			ready := v.Id.Time.Add(v.Id.Resolution).Add(time.Second)
			if !ready.After(schedule) {
				delete(m.m, k)
				out <- v
			}
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

func (m *MemStore) Put(b *bucket.Bucket) error {
	m.Lock()
	defer m.Unlock()
	if _, present := m.m[*b.Id]; !present {
		m.m[*b.Id] = b
	} else {
		m.m[*b.Id].Add(b)
	}
	return nil
}
