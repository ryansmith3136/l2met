package store

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"l2met/utils"
	"log"
)

var redisPool *redis.Pool

func init() {
	var err error
	host, password, err := utils.ParseRedisUrl()
	if err != nil {
		log.Fatal(err)
	}
	redisPool = &redis.Pool{
		MaxIdle:     60,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", host, time.Second, time.Second, time.Second)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", password)
			return c, err
		},
	}
}

func PingRedis() error {
	rc := redisPool.Get()
	defer rc.Close()
	_, err := rc.Do("PING")
	return err
}
