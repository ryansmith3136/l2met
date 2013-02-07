package encoding

import (
	"errors"
	"l2met/utils"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	msgPat = regexp.MustCompile(`( *)([a-zA-Z0-9\_\-\.]+)=?(([a-zA-Z0-9\.\-\_\.]+)|("([^\"]+)"))?`)
)

func ParseMsgData(msg []byte) (map[string]string, error) {
	d := make(map[string]string)
	pairs := msgPat.FindAllStringSubmatch(string(msg), -1)
	for i := range pairs {
		k := pairs[i][2]
		v := pairs[i][4]
		v2 := pairs[i][6]
		if len(v) == 0 {
			v = v2
		}
		d[k] = v
	}
	return d, nil
}

func DecodeArray(b []byte, dest *[]float64) error {
	defer utils.MeasureT(time.Now(), "encoding.decode-array")
	if len(b) < 2 {
		return errors.New("l2met/parser: Not able to decode array.")
	}
	if b[0] != '[' {
		return errors.New("l2met/parser: Not able to decode array.")
	}
	if b[len(b)-1] != ']' {
		return errors.New("l2met/parser: Not able to decode array.")
	}
	// pq returns something like: {1.0, 2.0}
	// let us remove the { and the }
	trimed := b[1:(len(b) - 1)]
	// Assuming the numbers are seperated by commas.
	numbers := strings.Split(string(trimed), " ")
	// Showing that we can do cool things with floats.
	for _, x := range numbers {
		f, err := strconv.ParseFloat(x, 64)
		if err == nil {
			*dest = append(*dest, f)
		}
	}
	return nil
}

func EncodeArray(arr []float64) []byte {
	// Convert our array into a string that looks like: {1,2,3.0}
	var d []byte
	d = append(d, '{')
	for i, f := range arr {
		st := strconv.FormatFloat(f, 'f', 5, 64)
		for _, s := range st {
			d = append(d, byte(s))
		}
		if i != (len(arr) - 1) {
			d = append(d, ',')
		}
	}
	d = append(d, '}')
	return d
}
