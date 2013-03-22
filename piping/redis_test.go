package piping

import (
	"l2met/store"
	"testing"
	"time"
)

//Emptys redis instance and tests a single bucket on a single partition
func TestSingleBucketToRedis(t *testing.T) {
	store.FlushEverything()
	b := GenerateBucket()
	store.PutBucket(b, "testmail", 1)
	buckets, _ := store.EmptyMailboxPartition("testmail", 0)
	if len(buckets) != 1 {
		t.Fatalf("emptyMailbox returned an empty set")
	}
	if !Compare(buckets[0], b) {
		t.FailNow()
	}
}

func TestBucketsToMultipleMailboxes(t *testing.T) {
	store.FlushEverything()
	b1 := GenerateBucket()
	b2 := GenerateBucket()
	b3 := GenerateBucket()

	store.PutBucket(b1, "test1", 1)
	store.PutBucket(b2, "test2", 1)
	store.PutBucket(b3, "test3", 1)

	bs1, _ := store.EmptyMailboxPartition("test1", 0)
	bs2, _ := store.EmptyMailboxPartition("test2", 0)
	bs3, _ := store.EmptyMailboxPartition("test3", 0)

	if len(bs1) != 1 || len(bs2) != 1 || len(bs3) != 1 {
		t.FailNow()
	}

	if !Compare(bs1[0], b1) || !Compare(bs2[0], b2) || !Compare(bs3[0], b3) {
		t.FailNow()
	}
}

func TestBucketsToMultiplePartitions(t *testing.T) {
	store.FlushEverything()
	b1 := GenerateBucket()
	b2 := GenerateBucket()
	b3 := GenerateBucket()

	store.PutBucket(b1, "test", 3)
	store.PutBucket(b2, "test", 3)
	store.PutBucket(b3, "test", 3)

	buckets, _ := store.EmptyMailboxPartition("test", 0)
	buckets1, _ := store.EmptyMailboxPartition("test", 1)
	buckets2, _ := store.EmptyMailboxPartition("test", 2)
	if len(buckets)+len(buckets1)+len(buckets2) != 3 {
		t.FailNow()
	}

}

func TestSingleRedis(t *testing.T) {
	store.FlushEverything()
	testSource := NewRandomSource(50)
	numPartitions := uint64(1)
	lockTTL := uint64(30)
	fetchInterval := uint64(5)
	redisOutlet := NewRedisOutlet(testSource.sender.GetOutput(), numPartitions, lockTTL, "testBox")
	redisSource := NewRedisSource(fetchInterval, numPartitions, lockTTL, "testBox")
	testOutlet := NewRandomOutlet(50, testSource.testList, redisSource.sender.NewOutputChannel("verifier", 50))
	testSource.Start()
	time.Sleep(20)
	redisOutlet.Start()
	redisSource.Start()
	testOutlet.Start()

	passes := 0
	fails := 0
	for p := range testOutlet.GetSuccessChan() {
		if p {
			passes++
		} else {
			fails++
		}
		if fails+passes > 49 {
			t.Logf("goodBuckets: %v", passes)
			t.Logf("badBuckets: %v", fails)

			if fails > 0 {
				t.FailNow()
			}
			//		println("testoutlet stop")
			testOutlet.Stop()
			//			println("redisoutlet stop")
			redisOutlet.Stop()
			redisSource.Stop()

			return
		}
	}
}

func TestMultiRedis(t *testing.T) {
	store.FlushEverything()
	testSource := NewRandomSource(100)
	numPartitions := uint64(2)
	lockTTL := uint64(30)
	fetchInterval := uint64(1)
	redisOutlet2 := NewRedisOutlet(testSource.sender.GetOutput(), numPartitions, lockTTL, "testBox")
	redisOutlet := NewRedisOutlet(testSource.sender.GetOutput(), numPartitions, lockTTL, "testBox")
	redisSource := NewRedisSource(fetchInterval, numPartitions, lockTTL, "testBox")
	testOutlet := NewRandomOutlet(100, testSource.testList, redisSource.GetOutput())
	testSource.Start()
	time.Sleep(20)
	redisOutlet2.Start()
	redisOutlet.Start()
	redisSource.Start()
	testOutlet.Start()
	passes := 0
	fails := 0
	for p := range testOutlet.GetSuccessChan() {
		if p {
			passes++
		} else {
			fails++
		}
		if fails+passes == 100 {
			t.Logf("goodBuckets: %v", passes)
			t.Logf("badBuckets: %v", fails)

			if fails > 0 {
				t.FailNow()
			}
			testOutlet.Stop()
			redisOutlet.Stop()
			redisSource.Stop()

			return
		}
	}
}
