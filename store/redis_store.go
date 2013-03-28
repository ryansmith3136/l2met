package store

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"hash/crc64"
	"l2met/bucket"
	"l2met/encoding"
	"l2met/utils"
	"time"
)

var PartitionTable = crc64.MakeTable(crc64.ISO)

type RedisStore struct {
	redisPool     *redis.Pool
	maxPartitions uint64
}

func NewRedisStore(server, pass string, maxPartitions uint64, maxConn int) *RedisStore {
	return &RedisStore{
		maxPartitions: maxPartitions,
		redisPool:     initRedisPool(server, pass, maxConn),
	}
}

func initRedisPool(server, pass string, maxConn int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxConn,
		IdleTimeout: 30 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", pass)
			return c, err
		},
	}
}

func (s *RedisStore) Health() bool {
	rc := s.redisPool.Get()
	defer rc.Close()
	_, err := rc.Do("PING")
	if err != nil {
		return false
	}
	return true
}

func (s *RedisStore) Scan(partition string) <-chan *bucket.Bucket {
	retBuckets := make(chan *bucket.Bucket)
	go func(out chan *bucket.Bucket) {
		rc := s.redisPool.Get()
		defer rc.Close()
		defer close(out)
		rc.Send("MULTI")
		rc.Send("SMEMBERS", partition)
		rc.Send("DEL", partition)
		reply, err := redis.Values(rc.Do("EXEC"))
		if err != nil {
			fmt.Printf("at=%q error=%s\n", "bucket-store-scan", err)
			return
		}
		var delCount int64
		var members []string
		redis.Scan(reply, &members, &delCount)
		for _, member := range members {
			id, err := bucket.ParseId(member)
			if err != nil {
				fmt.Printf("at=%q error=%s\n", "bucket-store-parse-key", err)
				continue
			}
			out <- &bucket.Bucket{Id: id}
		}
	}(retBuckets)
	return retBuckets
}

func (s *RedisStore) Put(b *bucket.Bucket) error {
	defer utils.MeasureT("bucket.put", time.Now())

	rc := s.redisPool.Get()
	defer rc.Close()

	b.Lock()
	key := b.Id.String()
	value := b.Vals
	b.Unlock()

	//TODO(ryandotsmith): Ensure consistent keys are being written.
	partition := s.bucketPartition("outlet", []byte(key))
	rc.Send("MULTI")
	rc.Send("RPUSH", key, value)
	rc.Send("EXPIRE", key, 300)
	rc.Send("SADD", partition, key)
	rc.Send("EXPIRE", partition, 300)
	_, err := rc.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func (s *RedisStore) Get(b *bucket.Bucket) error {
	defer utils.MeasureT("bucket.get", time.Now())

	rc := s.redisPool.Get()
	defer rc.Close()

	//Fill in the vals.
	reply, err := redis.Values(rc.Do("LRANGE", b.Id.String(), 0, -1))
	if err != nil {
		return err
	}
	for _, item := range reply {
		v, ok := item.([]byte)
		if !ok {
			continue
		}
		err = encoding.DecodeArray(v, &b.Vals, '[', ']', ' ')
	}
	return nil
}

func (s *RedisStore) bucketPartition(prefix string, b []byte) string {
	check := crc64.Checksum(b, PartitionTable)
	return fmt.Sprintf("%s.%d", prefix, check%s.maxPartitions)
}
