package utils

import (
	"testing"
	"time"
)

func flushRedis(t *testing.T) {
	_, err := rc.Do("flushall")
	if err != nil {
		t.Fatal(err)
	}
}

func TestLockMultiPartition(t *testing.T) {
	flushRedis(t)
	var critical = 0

	go func() {
		_, err := LockPartition("test", 2, 5)
		if err != nil {
			t.Fatal(err)
		}
		critical++
	}()
	time.Sleep(time.Millisecond * 100)

	go func() {
		_, err := LockPartition("test", 2, 5)
		if err != nil {
			t.Fatal(err)
		}
		critical++
	}()
	time.Sleep(time.Millisecond * 100)

	go func() {
		_, err := LockPartition("test", 2, 5)
		if err != nil {
			t.Fatal(err)
		}
		critical++
	}()
	time.Sleep(time.Millisecond * 100)

	if critical != 2 {
		t.Fatal("Critical section entered 3 times. Expected 2.")
	}
}
