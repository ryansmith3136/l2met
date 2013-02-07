package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"net/url"
	"os"
	"time"
)

var redisPool *redis.Pool

func init() {
	u, err := url.Parse(os.Getenv("REDISGREEN_URL"))
	if err != nil {
		fmt.Printf("error=%q\n", "Missing REDISGREEN_URL.")
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
		IdleTimeout: 1 * time.Hour,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", password)
			return c, err
		},
	}
}
