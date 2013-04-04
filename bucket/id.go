package bucket

import (
	"errors"
	"l2met/utils"
	"strconv"
	"strings"
	"time"
)

// TODO(ryandotsmith): This is an awful hack.
// It is typical to use a `:` to compose keys in redis,
// however, it is possible for a Id.Name to have a `:`.
// Thus we pick a very unlikely char to compose keys in redis.
const keySep = "→"

type Id struct {
	Time       time.Time
	Resolution time.Duration
	Token      string
	Name       string
	Units      string
	Source     string
}

func ParseId(s string) (*Id, error) {
	parts := strings.Split(s, keySep)
	if len(parts) < 5 {
		return nil, errors.New("bucket: Unable to parse bucket key.")
	}

	t, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}

	ts := time.Unix(t, 0)
	if err != nil {
		return nil, err
	}

	res, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}

	id := new(Id)
	id.Time = ts
	id.Resolution = time.Duration(res)
	id.Token = parts[2]
	id.Name = parts[3]
	id.Units = parts[4]
	if len(parts) > 5 {
		id.Source = parts[5]
	}
	return id, nil
}

func (id *Id) String() string {
	s := ""
	s += strconv.FormatInt(id.Time.Unix(), 10) + keySep
	s += strconv.FormatInt(int64(id.Resolution), 10) + keySep
	s += id.Token + keySep
	s += id.Name + keySep
	s += id.Units
	if len(id.Source) > 0 {
		s += keySep + id.Source
	}
	return s
}

func (id *Id) Delay(t time.Time) int64 {
	t0 := utils.RoundTime(id.Time, id.Resolution).Unix()
	t1 := utils.RoundTime(t, id.Resolution).Unix()
	base := id.Resolution / time.Second
	if base != 0 {
		return (t1 - t0) / int64(base)
	}
	return 0
}
