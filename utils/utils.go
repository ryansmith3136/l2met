package utils

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	appName string
)

func init() {
	appName = os.Getenv("APP_NAME")
	if len(appName) == 0 {
		fmt.Println("Must set APP_NAME.")
		os.Exit(1)
	}
}

func EnvUint64(name string, defaultVal uint64) uint64 {
	tmp := os.Getenv(name)
	if len(tmp) != 0 {
		n, err := strconv.ParseUint(tmp, 10, 64)
		if err == nil {
			return n
		}
	}
	return defaultVal
}

func EnvString(name string, defaultVal string) string {
	tmp := os.Getenv(name)
	if len(tmp) != 0 {
		return tmp
	}
	return defaultVal
}

func EnvInt(name string, defaultVal int) int {
	tmp := os.Getenv(name)
	if len(tmp) != 0 {
		n, err := strconv.Atoi(tmp)
		if err == nil {
			return n
		}
	}
	return defaultVal
}

func MeasureI(measurement string, val int64) {
	m := fmt.Sprintf("%s.%s", appName, measurement)
	fmt.Printf("measure=%q val=%d\n", m, val)
}

func MeasureT(measurement string, t time.Time) {
	m := fmt.Sprintf("%s.%s", appName, measurement)
	fmt.Printf("measure=%q val=%d\n", m, time.Since(t)/time.Millisecond)
}

func WriteJsonBytes(w http.ResponseWriter, status int, b []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	w.Write(b)
	w.Write([]byte("\n"))
}

// Convenience
func WriteJson(w http.ResponseWriter, status int, data interface{}) {
	b, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		WriteJson(w, 500, map[string]string{"error": "Internal Server Error"})
	}
	WriteJsonBytes(w, status, b)
}

func RoundTime(t time.Time, d time.Duration) time.Time {
	return time.Unix(0, int64((time.Duration(t.UnixNano())/d)*d))
}

func ParseToken(r *http.Request) (string, error) {
	header, ok := r.Header["Authorization"]
	if !ok {
		return "", errors.New("Authorization header not set.")
	}

	auth := strings.SplitN(header[0], " ", 2)
	if len(auth) != 2 {
		return "", errors.New("Malformed header.")
	}

	userPass, err := base64.StdEncoding.DecodeString(auth[1])
	if err != nil {
		return "", errors.New("Malformed encoding.")
	}

	parts := strings.Split(string(userPass), ":")
	if len(parts) != 2 {
		return "", errors.New("Password not supplied.")
	}

	return parts[1], nil
}

func LockPartition(ns string, max, lockTTL uint64) (uint64, error) {
	for {
		var p uint64
		for p = 0; p < max; p++ {
			value := fmt.Sprintf("%s.%d", ns, p)
			rc := redisPool.Get()
			reply, err := redis.Int(rc.Do("SADD", "partitions", value))
			if err != nil {
				fmt.Printf("error=%q\n", err)
				rc.Close()
				continue
			}

			if reply == 0 {
				rc.Close()
				continue
			}
			reply, err = redis.Int(rc.Do("expire", value, lockTTL))

			if err != nil {
				fmt.Printf("error=%q\n", err)
				rc.Close()
				continue
			}

			//Heartbeat is intentionally unstoppable. 
			//We assume that the heartbeat continues as long 
			//as the program has not crashed, or locked up
			go func() {
				last := time.Now().Unix()

				for {
					//We want to at least get one heartbeat per 
					//interval, so we are dividing the lock_ttl by 4
					time.Sleep(time.Duration(lockTTL * 250))
					if (time.Now().Unix() - last) > int64((time.Second * time.Duration(lockTTL))) {
						panic("Lock has been lost.")
					}
					reply, err := redis.Int(rc.Do("expire", value, lockTTL))
					if (-1 == reply) || err != nil {
						panic(fmt.Sprintf("Unable to set expire on lock. error=%s\n", err))
					}
					last = time.Now().Unix()
				}
			}()

			return p, nil
		}
		fmt.Printf("at=%q\n", "waiting-for-partition-lock")
		time.Sleep(time.Second * 10)
	}
	return 0, errors.New("Unable to lock partition.")
}
