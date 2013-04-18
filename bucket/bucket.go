package bucket

import (
	"bufio"
	"fmt"
	"github.com/bmizerany/logplex"
	"io"
	"l2met/encoding"
	"l2met/utils"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DefaultUnit = "u"

type Bucket struct {
	// A bucket can be locked to ensure safe memory access.
	sync.Mutex
	// The identity of a bucket is used in registers and as keys in redis.
	Id *Id
	// A slice of all the measurements for a bucket.
	Vals []float64 `json:"vals,omitempty"`
}

//TODO(ryandotsmith): NewBucket should be broken up. This func is too big.
func NewBucket(user, pass string, rdr *bufio.Reader, opts map[string][]string) <-chan *Bucket {
	//TODO(ryandotsmith): Can we eliminate the magical number?
	buckets := make(chan *Bucket, 10000)
	go func(c chan<- *Bucket) {
		defer close(c)
		lp := logplex.NewReader(rdr)
		for {
			logLine, err := lp.ReadMsg()
			if err != nil {
				if err == io.EOF {
					break
				}
				fmt.Printf("at=logplex-error err=%s\n", err)
				return
			}

			logData, err := encoding.ParseMsgData(logLine.Msg)
			if err != nil {
				continue
			}

			ts, err := logLine.Time()
			if err != nil {
				fmt.Printf("at=time-error error=%s\n", err)
				continue
			}

			//The resolution determines how long a bucket is
			//left to linger. E.g. a bucket with 1 second resolution
			//will hang around for 1 second and provide metrics
			//with 1 second resolution.
			resQuery, ok := opts["resolution"]
			if !ok {
				resQuery = []string{"60"}
			}
			resTmp, err := strconv.Atoi(resQuery[0])
			if err != nil {
				continue
			}
			res := time.Duration(time.Second * time.Duration(resTmp))
			ts = utils.RoundTime(ts, res)

			//Src can be overridden by the heroku router messages.
			src := logData["source"]

			//You can prefix all measurments by adding the
			//prefix option on your drain url.
			var prefix string
			if prefixQuery, ok := opts["prefix"]; ok {
				if len(prefixQuery[0]) > 0 {
					prefix = prefixQuery[0] + "."
				}
			}

			//Special case the Heroku router.
			//In this case, we will massage logData
			//to include connect, service, and bytes.
			if string(logLine.Pid) == "router" {
				p := "measure.router."
				if len(logData["host"]) > 0 {
					src = logData["host"]
				}
				if len(logData["connect"]) > 0 {
					logData[p+"connect"] = logData["connect"]
				}
				if len(logData["service"]) > 0 {
					logData[p+"service"] = logData["service"]
				}
				if len(logData["bytes"]) > 0 {
					logData[p+"bytes"] = logData["bytes"] + "bytes"
				}
			}

			for k, v := range logData {
				switch k {
				//TODO(ryandotsmith): this case is measre=something val=x
				//It is deprecated and not mentioned in the docs.
				//We should remove this sometime in the near future.
				case "measure":
					units, val := parseVal(logData["val"])
					name := prefix + v
					id := &Id{ts, res, user, pass, name, units, src}
					bucket := &Bucket{Id: id}
					bucket.Vals = []float64{val}
					c <- bucket
				default:
					if !strings.HasPrefix(k, "measure.") {
						break
					}
					name := prefix + k[8:] // len("measure.") == 8
					units, val := parseVal(v)
					id := &Id{ts, res, user, pass, name, units, src}
					bucket := &Bucket{Id: id}
					bucket.Vals = []float64{val}
					c <- bucket
				}
			}
		}
	}(buckets)
	return buckets
}

var (
	unitsPat = regexp.MustCompile(`[a-zA-Z]+`)
	valPat   = regexp.MustCompile(`[^a-zA-z]+`)
)

// If we can parse a number from the string,
// we will return that number. Return 1 otherwise.
func parseVal(s string) (string, float64) {
	var units string
	var val float64

	units = unitsPat.FindString(s)
	if len(units) == 0 {
		units = DefaultUnit
	}

	val = float64(1)
	tmpVal := valPat.FindString(s)
	if len(tmpVal) > 0 {
		v, err := strconv.ParseFloat(tmpVal, 64)
		if err == nil {
			val = v
		}
	}
	return units, val
}

// Adding bucket a to bucket b copies the vals of bucket b and
// appends them to the vals of bucket a. Nothing is done with the
// Ids of the buckets. You should ensure that buckets have the same Id.
func (b *Bucket) Add(otherM *Bucket) {
	b.Lock()
	defer b.Unlock()
	for _, v := range otherM.Vals {
		b.Vals = append(b.Vals, v)
	}
}

/*
 The remainder of these methods provide statistics for buckets.
*/

func (b *Bucket) Count() int {
	return len(b.Vals)
}

func (b *Bucket) Sum() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	s := float64(0)
	for i := range b.Vals {
		s += b.Vals[i]
	}
	return s
}

func (b *Bucket) Mean() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	return b.Sum() / float64(b.Count())
}

func (b *Bucket) Sort() {
	if !sort.Float64sAreSorted(b.Vals) {
		sort.Float64s(b.Vals)
	}
}

func (b *Bucket) Min() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	return b.Vals[0]
}

func (b *Bucket) Median() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Ceil(float64(b.Count() / 2)))
	return b.Vals[pos]
}

func (b *Bucket) P95() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.95))
	return b.Vals[pos]
}

func (b *Bucket) P99() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.99))
	return b.Vals[pos]
}

func (b *Bucket) Max() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := b.Count() - 1
	return b.Vals[pos]
}

func (b *Bucket) Last() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	pos := b.Count() - 1
	return b.Vals[pos]
}
