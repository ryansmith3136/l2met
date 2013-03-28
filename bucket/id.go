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
	Time   time.Time
}

func ParseId(s string) (*Id, error) {
	parts := strings.Split(s, keySep)
	if len(parts) < 3 {
		return nil, errors.New("bucket: Unable to parse bucket key.")
	}

	t, err := strconv.ParseInt(parts[0], 10, 54)
	if err != nil {
		return nil, err
	}

	time := time.Unix(t, 0)
	if err != nil {
		return nil, err
	}

	id := new(Id)
	id.Time = time
	id.Token = parts[1]
	id.Name = parts[2]
	if len(parts) > 3 {
		id.Source = parts[3]
	}
	return id, nil
}

func (id *Id) String() string {
	s := ""
	s += strconv.FormatInt(id.Time.Unix(), 10) + keySep
	s += id.Token + keySep
	s += id.Name
	if len(id.Source) > 0 {
		s += keySep + id.Source
	}
	return s
}
