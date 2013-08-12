package bucket

import (
	"bytes"
	"encoding/gob"
	"time"
)

type Id struct {
	Time       time.Time
	Resolution time.Duration
	Auth       string
	Name       string
	Units      string
	Source     string
	Type       string
}

func (id *Id) Decode(b *bytes.Buffer) error {
	dec := gob.NewDecoder(b)
	return dec.Decode(id)
}

func (id *Id) Encode() ([]byte, error) {
	var res bytes.Buffer
	enc := gob.NewEncoder(&res)
	err := enc.Encode(id)
	return res.Bytes(), err
}

// The number of time units returned represents
// the processing time accumulated within l2met.
// E.g. If the resolution of the bucket/id is 60s
// and the delay is 2, then it took 120s for l2met
// to process the bucket.
func (id *Id) Delay(t time.Time) int64 {
	t0 := id.Time.Round(id.Resolution).Unix()
	t1 := t.Round(id.Resolution).Unix()
	base := id.Resolution / time.Second
	if base != 0 {
		return (t1 - t0) / int64(base)
	}
	return 0
}
