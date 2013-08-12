package store

import (
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/redisync"
	"testing"
	"time"
)

func TestRedisGet(t *testing.T) {
	cfg := &conf.D{MaxPartitions: 1, RedisHost: "localhost:6379"}
	st := NewRedisStore(cfg)
	st.Mchan = new(metchan.Channel)
	st.Flush()
	id := &bucket.Id{Name: "test"}
	b1 := &bucket.Bucket{
		Id:   id,
		Vals: []float64{99.99999, 1, 0.2},
	}
	if err := st.Put(b1); err != nil {
		t.Error(err)
		t.FailNow()
	}

	b2 := &bucket.Bucket{Id: id}
	if err := st.Get(b2); err != nil {
		t.Error(err)
	}
	if len(b2.Vals) != len(b1.Vals) {
		t.Error("Expected size of b1 & b2 to be equal.")
		t.FailNow()
	}
	for i := range b1.Vals {
		if b1.Vals[i] != b2.Vals[i] {
			t.Errorf("b1[%d]= %f and b2[%d] = %f",
				i, b1.Vals[i], i, b2.Vals[i])
		}
	}
}

func TestRedisScan(t *testing.T) {
	cfg := &conf.D{MaxPartitions: 1, RedisHost: "localhost:6379"}
	st := NewRedisStore(cfg)
	st.Mchan = new(metchan.Channel)
	st.Flush()

	schedule := time.Now()
	id := &bucket.Id{
		Name:       "test",
		Time:       time.Now().Add(-1 * time.Second),
		Resolution: time.Second,
		ReadyAt:    schedule,
	}
	b1 := &bucket.Bucket{
		Id:   id,
		Vals: []float64{99.99999, 1, 0.2},
	}
	st.Put(b1)
	bchan, err := st.Scan(schedule)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	var buckets []*bucket.Bucket
	for b := range bchan {
		buckets = append(buckets, b)
	}
	if len(buckets) != 1 {
		t.Errorf("expected=1 actual=%d\n", len(buckets))
		t.FailNow()
	}
	if buckets[0].Id.Name != id.Name {
		t.Errorf("expected id to be equal.")
	}
}

func TestRedisLockPartition(t *testing.T) {
	cfg := &conf.D{MaxPartitions: 1, RedisHost: "localhost:6379"}
	st := NewRedisStore(cfg)
	st.Mchan = new(metchan.Channel)
	st.Flush()

	done := make(chan *redisync.Mutex)
	wait := time.After(time.Second)
	go func() {
		rc := st.redisPool.Get()
		defer rc.Close()
		mut, _ := st.lockPartition(rc)
		done <- mut
	}()
	select {
	case <-done:
	case <-wait:
		t.Errorf("Unable to lock partition.")
	}
}
