package receiver

import (
	"l2met/bucket"
	"l2met/store"
	"testing"
	"time"
)

func makeReceiver() (store.Store, *Receiver) {
	store := store.NewMemStore()

	recv := NewReceiver()
	recv.MaxOutbox = 100
	recv.MaxInbox = 100
	recv.FlushInterval = 1
	recv.NumOutlets = 2
	recv.NumAcceptors = 2
	recv.Store = store

	return store, recv
}

func TestReceive(t *testing.T) {
	store, recv := makeReceiver()
	recv.Start(time.Nanosecond)
	defer recv.Stop()

	opts := map[string][]string{}
	msg := []byte("81 <190>1 2013-03-27T20:02:24+00:00 hostname token shuttle - - measure=hello val=99\n")
	recv.Receive("123", msg, opts)
	time.Sleep(time.Millisecond)

	var buckets []*bucket.Bucket
	for bucket := range store.Scan("not important") {
		buckets = append(buckets, bucket)
	}

	if len(buckets) != 1 {
		t.FailNow()
	}

	testBucket := buckets[0]
	if testBucket.Id.Name != "hello" {
		t.FailNow()
	}

	if testBucket.Sum() != 99 {
		t.FailNow()
	}
}

func TestReceiveOpts(t *testing.T) {
	store, recv := makeReceiver()
	recv.Start(time.Nanosecond)
	defer recv.Stop()

	opts := map[string][]string{"bucket-size": []string{"1000"}}
	msg := []byte("81 <190>1 2013-03-27T00:00:01+00:00 hostname token shuttle - - measure=hello val=99\n")
	recv.Receive("123", msg, opts)
	time.Sleep(time.Millisecond)

	var buckets []*bucket.Bucket
	for bucket := range store.Scan("not important") {
		buckets = append(buckets, bucket)
	}

	if len(buckets) != 1 {
		t.FailNow()
	}

	testBucket := buckets[0]
	if testBucket.Id.Name != "hello" {
		t.FailNow()
	}

	if testBucket.Sum() != 99 {
		t.FailNow()
	}

	if testBucket.Id.Time.Second() != 1 {
		t.FailNow()
	}
}
