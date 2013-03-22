package store

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"l2met/utils"
	"net/url"
	"os"
	"time"
)

var redisPool *redis.Pool

func init() {
	u, err := url.Parse(os.Getenv("REDIS_URL"))
	if err != nil {
		fmt.Printf("error=%q\n", "Missing REDIS_URL.")
		os.Exit(1)
	}
	server := u.Host
	password, set := u.User.Password()
	if !set {
		fmt.Printf("at=error error=%q\n", "password not set")
		os.Exit(1)
	}
	redisPool = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", server, time.Second, time.Second, time.Second)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", password)
			return c, err
		},
	}
}

//Check if redis is alive
func PingRedis() error {
	rc := redisPool.Get()
	defer rc.Close()
	_, err := rc.Do("PING")
	return err
}

//Put a single bucket into redis 
func PutBucket(b *Bucket, mailbox string, numPartitions uint64) {
	defer utils.MeasureT("redis.bucket.put", time.Now())

	b.Lock()
	vals := b.Vals
	key := b.String()
	//It might make since for this to be relegated to the RedisPartitioner class
	partition := b.Partition([]byte(key), numPartitions)
	b.Unlock()

	rc := redisPool.Get()
	defer rc.Close()
	mailBox := fmt.Sprintf("%s.%d", mailbox, partition)

	rc.Send("MULTI")
	rc.Send("RPUSH", key, vals)
	rc.Send("EXPIRE", key, 300)
	rc.Send("SADD", mailBox, key)
	rc.Send("EXPIRE", mailBox, 300)
	rc.Do("EXEC")

	//Some sort of reporting should be happening here
	//if err != nil {   
	//}
}

//clears the redis DB
func FlushEverything() {
	rc := redisPool.Get()
	rc.Send("FlushDB")
	rc.Do("EXEC")
}

//Trys to get a lock on a givin mailbox and partition to be held for the ttl time
//Returns true if if lock was aquired and false, error if it fails
func TryLock(mailbox string, partition uint64, ttl uint64) (bool, error) {
	rc := redisPool.Get()
	defer rc.Close()
	lockString := fmt.Sprintf("lock.%s.%d", mailbox, partition)

	new := time.Now().Unix() + int64(ttl) + 1
	old, err := redis.Int(rc.Do("GETSET", lockString, new))
	// If the ErrNil is present, the old value is set to 0.
	if err != nil && err != redis.ErrNil && old == 0 {
		return false, err
	}
	// If the new value is greater than the old
	// value, then the old lock is expired.
	return new > int64(old), nil
}

//Get all the buckets out of a givin mailbox and partition
func EmptyMailboxPartition(mailbox string, partition int) (buckets []*Bucket, deleteCount int64) {
	rc := redisPool.Get()
	defer rc.Close()
	mailbox = fmt.Sprintf("%s.%d", mailbox, partition)
	rc.Send("MULTI")
	rc.Send("SMEMBERS", mailbox)
	rc.Send("DEL", mailbox)
	reply, err := redis.Values(rc.Do("EXEC"))

	if err != nil {
		fmt.Printf("at=%q error%s\n", "redset-smembers", err)
		return
	}
	var members []string
	redis.Scan(reply, &members, &deleteCount)
	buckets = make([]*Bucket, len(members), len(members))
	for i, member := range members {
		k, _ := ParseKey(member)
		buckets[i] = &Bucket{Key: *k}
	}

	utils.MeasureI("redis.emptyMailbox.members", int64(len(members)))

	return
}

//Here for semetry, There is also going to be a GetFromPG()
func (b *Bucket) GetFromRedis() error {
	return b.Get()
}
