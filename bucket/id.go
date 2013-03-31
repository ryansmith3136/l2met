package bucket

import (
	"errors"
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
	Token  string
	Name   string
	Source string
	Resolution time.Duration
	Time   time.Time
}

func ParseId(s string) (*Id, error) {
	parts := strings.Split(s, keySep)
	if len(parts) < 4 {
		return nil, errors.New("bucket: Unable to parse bucket key.")
	}

	t, err := strconv.ParseInt(parts[0], 10, 54)
	if err != nil {
		return nil, err
	}

	ts := time.Unix(t, 0)
	if err != nil {
		return nil, err
	}

	res, err := strconv.ParseInt(parts[1], 10, 54)
	if err != nil {
		return nil, err
	}

	id := new(Id)
	id.Time = ts
	id.Resolution = time.Duration(res)
	id.Token = parts[2]
	id.Name = parts[3]
	if len(parts) > 4 {
		id.Source = parts[5]
	}
	return id, nil
}

func (id *Id) String() string {
	s := ""
	s += strconv.FormatInt(id.Time.Unix(), 10) + keySep
	s += strconv.FormatInt(int64(id.Resolution), 10) + keySep
	s += id.Token + keySep
	s += id.Name
	if len(id.Source) > 0 {
		s += keySep + id.Source
	}
	return s
}
