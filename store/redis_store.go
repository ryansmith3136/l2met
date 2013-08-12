package store

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/redisync"
	"strconv"
	"time"
)

const (
	lockPrefix      = "lock"
	partitionPrefix = "partition.outlet"
)

func initRedisPool(cfg *conf.D) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     cfg.Concurrency,
		IdleTimeout: 30 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.RedisHost)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", cfg.RedisPass)
			return c, err
		},
	}
}

type RedisStore struct {
	redisPool     *redis.Pool
	maxPartitions uint64
	Mchan         *metchan.Channel
}

func NewRedisStore(cfg *conf.D) *RedisStore {
	return &RedisStore{
		maxPartitions: cfg.MaxPartitions,
		redisPool:     initRedisPool(cfg),
	}
}

func (s *RedisStore) MaxPartitions() uint64 {
	return s.maxPartitions
}

// Sends a PING request to Redis.
func (s *RedisStore) Health() bool {
	rc := s.redisPool.Get()
	defer rc.Close()
	_, err := rc.Do("PING")
	if err != nil {
		return false
	}
	return true
}

// Reads the TIME from Redis.
func (s *RedisStore) Now() time.Time {
	rc := s.redisPool.Get()
	defer rc.Close()
	defer s.Mchan.Time("store.time", time.Now())
	reply, err := redis.Values(rc.Do("TIME"))
	if err != nil {
		fmt.Printf("error=redis-time-not-available\n")
		return time.Now()
	}
	sec, err := strconv.Atoi(string(reply[0].([]byte)))
	if err != nil {
		fmt.Printf("error=redis-time-not-available\n")
		return time.Now()
	}
	microSec, err := strconv.Atoi(string(reply[1].([]byte)))
	if err != nil {
		fmt.Printf("error=redis-time-not-available\n")
		return time.Now()
	}
	return time.Unix(int64(sec), int64(microSec*1000))
}

func (s *RedisStore) Scan(schedule time.Time) (<-chan *bucket.Bucket, error) {
	out := make(chan *bucket.Bucket)
	rc := s.redisPool.Get()
	mut, n := s.lockPartition(rc)
	p := namePartition(schedule, n)
	go func() {
		defer s.Mchan.Time("store.scan", time.Now())
		defer rc.Close()
		defer mut.Unlock(rc)
		defer close(out)
		rc.Send("MULTI")
		rc.Send("SMEMBERS", p)
		rc.Send("DEL", p)
		reply, err := redis.Values(rc.Do("EXEC"))
		if err != nil {
			fmt.Printf("at=%q error=%s\n", "bucket-store-scan", err)
			return
		}
		var delCount int64
		var members []string
		redis.Scan(reply, &members, &delCount)
		for i := range members {
			id := new(bucket.Id)
			err := id.Decode(bytes.NewBufferString(members[i]))
			if err != nil {
				fmt.Printf("at=%q error=%s\n",
					"bucket-store-parse-key", err)
				continue
			}
			out <- &bucket.Bucket{Id: id}
		}
	}()
	return out, nil
}

func (s *RedisStore) Put(b *bucket.Bucket) error {
	defer s.Mchan.Time("store.put", time.Now())
	b.Lock()
	defer b.Unlock()
	rc := s.redisPool.Get()
	defer rc.Close()

	idBytes, err := b.Id.Encode()
	if err != nil {
		return err
	}
	payload := make([]interface{}, len(b.Vals)+1)
	payload[0] = idBytes
	for i := range b.Vals {
		x := strconv.FormatFloat(b.Vals[i], 'f', 10, 64)
		payload[i+1] = []byte(x)
	}

	p := namePartition(b.Id.ReadyAt, b.Id.Partition(s.maxPartitions))
	rc.Send("MULTI")
	rc.Send("RPUSH", payload...)
	rc.Send("EXPIRE", payload[0], 300)
	rc.Send("SADD", p, payload[0])
	rc.Send("EXPIRE", p, 300)
	_, err = rc.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func (s *RedisStore) Get(b *bucket.Bucket) error {
	defer s.Mchan.Time("store.get", time.Now())
	rc := s.redisPool.Get()
	defer rc.Close()

	key, err := b.Id.Encode()
	if err != nil {
		return err
	}
	reply, err := redis.Values(rc.Do("LRANGE", key, 0, -1))
	if err != nil {
		return err
	}
	if len(reply) == 0 {
		return errors.New("redis_store: Empty bucket.")
	}
	b.Vals = make([]float64, len(reply))
	for i := range reply {
		numstr := reply[i].([]byte)
		numf, err := strconv.ParseFloat(string(numstr), 64)
		if err == nil {
			b.Vals[i] = numf
		}

	}
	return nil
}

func namePartition(schedule time.Time, n uint64) string {
	return fmt.Sprintf("%d.%s.%d", schedule.Unix(), partitionPrefix, n)
}

func nameLock(n uint64) string {
	return fmt.Sprintf("%s.%d", lockPrefix, n)
}

// Sleeps until a lock can be acquired.
// Attempts all locks in the lock space. E.g. [0, maxPartitions)
func (s *RedisStore) lockPartition(c redis.Conn) (*redisync.Mutex, uint64) {
	for {
		for n := uint64(0); n < s.MaxPartitions(); n++ {
			mut := redisync.NewMutex(nameLock(n), time.Minute)
			if mut.TryLock(c) {
				s.Mchan.Measure("store.lock-success", 1)
				return mut, n
			}
			s.Mchan.Measure("store.lock-fail", 1)
		}
		time.Sleep(time.Second)
	}
}

func (s *RedisStore) Flush() {
	rc := s.redisPool.Get()
	defer rc.Close()
	rc.Do("FLUSHALL")
}
