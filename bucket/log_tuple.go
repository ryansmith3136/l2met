package bucket

import (
	"bytes"
	"errors"
	"strconv"
	"github.com/kr/logfmt"
)

type logTuple struct {
	Key []byte
	Val []byte
}

func (lt *logTuple) Name() string {
	return string(lt.Key)
}

func (lt *logTuple) Float64() (float64, error) {
	//If the caller is asking for the float value of a key
	//and the key is blank, we return a 1. It is idiomatic
	//for logs to contain data like: measure.hello. This is
	//interpreted by l2met as: measure.hello=1. That feature
	//is implemented here.
	if len(lt.Val) == 0 {
		lt.Val = []byte("1")
	}
	digits := make([]byte, 0)
	for i := range lt.Val {
		b := lt.Val[i]
		if b < '0' || b > '9' {
			break
		}
		digits = append(digits, b)
	}
	if len(digits) > 0 {
		v, err := strconv.ParseFloat(string(digits), 10)
		if err != nil {
			return 0, err
		}
		return v, nil
	}
	return 0, errors.New("Unable to parse float.")
}

func (lt *logTuple) String() string {
	return string(lt.Val)
}

func (lt *logTuple) Units() string {
	f, err := lt.Float64()
	if err != nil {
		return ""
	}
	fs := strconv.FormatFloat(f, 'g', 10, 64)
	fb := []byte(fs)
	units := lt.Val[len(fb):]
	return string(units)
}

type tuples []*logTuple

func (t *tuples) HandleLogfmt(k, v []byte) error {
	*t = append(*t, &logTuple{k, v})
	return nil
}

func (t *tuples) Source() string {
	for i := range *t {
		if bytes.Equal((*t)[i].Key, []byte("source")) {
			return (*t)[i].String()
		}
		//The Heroku router fills in the host key, if the host
		//is present, we will use this as the source.
		if bytes.Equal((*t)[i].Key, []byte("host")) {
			return (*t)[i].String()
		}
	}
	return ""
}

func parseLogData(msg []byte) (tuples, error) {
	tups := make(tuples, 0)
	if err := logfmt.Unmarshal(msg, &tups); err != nil {
		return nil, err
	}
	return tups, nil
}
