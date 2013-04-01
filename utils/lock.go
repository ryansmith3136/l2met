package utils

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

var rc redis.Conn

func init() {
	var err error
	host, password, err := ParseRedisUrl()
	if err != nil {
		log.Fatal(err)
	}
	rc, err = redis.Dial("tcp", host)
	if err != nil {
		log.Fatalf("Locking service is unable to connect to redis. err: %s",
			err)
	}
	rc.Do("AUTH", password)
}

func UnlockPartition(key string) {
	rc.Do("DEL", key)
}

func LockPartition(ns string, max, ttl uint64) (uint64, error) {
	for {
		for p := uint64(0); p < max; p++ {
			name := fmt.Sprintf("lock.%s.%d", ns, p)
			locked, err := writeLock(name, ttl)
			if err != nil {
				return 0, err
			}
			if locked {
				return p, nil
			}
		}
		time.Sleep(time.Second * 5)
	}
	return 0, errors.New("LockPartition impossible broke the loop.")
}

func writeLock(name string, ttl uint64) (bool, error) {
	new := time.Now().Unix() + int64(ttl) + 1
	old, err := redis.Int(rc.Do("GETSET", name, new))
	// If the ErrNil is present, the old value is set to 0.
	if err != nil && err != redis.ErrNil && old == 0 {
		return false, err
	}
	// If the new value is greater than the old
	// value, then the old lock is expired.
	return new > int64(old), nil
}
