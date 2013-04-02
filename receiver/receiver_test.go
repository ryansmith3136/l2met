package receiver

import (
	"l2met/bucket"
	"l2met/store"
	"testing"
	"time"
)

func makeReceiver() (store.Store, *Receiver) {
	store := store.NewMemStore()

	maxOutbox := 100
	maxInbox := 100
	recv := NewReceiver(maxInbox, maxOutbox)
	recv.NumOutlets = 2
	recv.NumAcceptors = 2
	recv.Store = store

	return store, recv
}

func TestReceive(t *testing.T) {
	store, recv := makeReceiver()
	recv.FlushInterval = time.Millisecond
	recv.Start()
	defer recv.Stop()

	opts := map[string][]string{}
	msg := []byte("81 <190>1 2013-03-27T20:02:24+00:00 hostname token shuttle - - measure=hello val=99\n")
	recv.Receive("123", msg, opts)
	time.Sleep(recv.FlushInterval * 2)

	var buckets []*bucket.Bucket
	for b := range store.Scan("not important") {
		buckets = append(buckets, b)
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
	recv.FlushInterval = time.Millisecond
	recv.Start()
	defer recv.Stop()

	opts := map[string][]string{"resolution": []string{"1000"}}
	msg := []byte("81 <190>1 2013-03-27T00:00:01+00:00 hostname token shuttle - - measure=hello val=99\n")
	recv.Receive("123", msg, opts)
	time.Sleep(recv.FlushInterval * 2)

	var buckets []*bucket.Bucket
	for b := range store.Scan("not important") {
		buckets = append(buckets, b)
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

func TestReceiveMultiMetrics(t *testing.T) {
	store, recv := makeReceiver()
	recv.FlushInterval = time.Millisecond
	recv.Start()
	defer recv.Stop()

	opts := map[string][]string{"resolution": []string{"1000"}}
	msg := []byte("95 <190>1 2013-03-27T00:00:01+00:00 hostname token shuttle - - measure=hello val=10 measure.db=10\n")
	recv.Receive("123", msg, opts)
	time.Sleep(recv.FlushInterval * 2)

	var buckets []*bucket.Bucket
	for b := range store.Scan("not important") {
		buckets = append(buckets, b)
	}

	expectedLength := 2
	actualLength := len(buckets)
	if actualLength != expectedLength {
		t.Errorf("expected=%d actual=%d\n", expectedLength, actualLength)
	}

	//the log line above has two measurements with values of 10.
	actualSum := float64(0)
	for i := range buckets {
		actualSum += buckets[i].Sum()
	}
	expectedSum := float64(20)
	if actualSum != expectedSum {
		t.Errorf("expected=%d actual=%d\n", expectedSum, actualSum)
	}
}

func TestReceiveRouter(t *testing.T) {
	store, recv := makeReceiver()
	recv.FlushInterval = time.Millisecond
	recv.Start()
	defer recv.Stop()

	opts := map[string][]string{"resolution": []string{"1000"}}
	msg := []byte("112 <190>1 2013-03-27T00:00:01+00:00 router token shuttle - - host=test.l2met.net service=10ms connect=10ms bytes=45")
	recv.Receive("123", msg, opts)
	time.Sleep(recv.FlushInterval * 2)

	var buckets []*bucket.Bucket
	for b := range store.Scan("not important") {
		buckets = append(buckets, b)
	}

	expectedLength := 3
	actualLength := len(buckets)
	if actualLength != expectedLength {
		t.Errorf("expected=%d actual=%d\n", expectedLength, actualLength)
	}

	//the log line above has two measurements with values of 10.
	actualSum := float64(0)
	for i := range buckets {
		actualSum += buckets[i].Sum()
	}
	expectedSum := float64(65)
	if actualSum != expectedSum {
		t.Errorf("expected=%d actual=%d\n", expectedSum, actualSum)
	}
}
