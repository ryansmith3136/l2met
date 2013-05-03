package bucket

import (
	"bufio"
	"fmt"
	"github.com/ryandotsmith/lpx"
	"strconv"
	"strings"
	"time"
)

type Options map[string][]string

var (
	routerPrefix  = "router"
	measurePrefix = "measure."
)

func NewBuckets(body *bufio.Reader, opts Options) <-chan *Bucket {
	rdr := lpx.NewReader(body)
	out := make(chan *Bucket)
	go func(o chan *Bucket) {
		defer close(o)
		for rdr.Next() {
			header := rdr.Header()
			msg := rdr.Bytes()
			tups, err := parseLogData(msg)
			if err != nil {
				fmt.Printf("at=parse-log-data error=%s\n", err)
				continue
			}
			parseHkRouter(o, opts, header, tups)
			parseMeasurements(o, opts, header, tups)
		}
	}(out)
	return out
}

func parseMeasurements(out chan *Bucket, opts Options, header *lpx.Header, tups tuples) error {
	for i := range tups {
		if !strings.HasPrefix(tups[i].Name(), measurePrefix) {
			continue
		}
		id := new(Id)
		id.Resolution = parseResolution(opts)
		id.Time = parseTime(id.Resolution, header.Time)
		id.User = opts["user"][0]
		id.Pass = opts["password"][0]
		id.Name = buildPrefix(opts, tups[i].Name())
		id.Units = tups[i].Units()
		id.Source = tups.Source()
		val, err := tups[i].Float64()
		if err != nil {
			continue
		}
		out <- &Bucket{Id: id, Vals: []float64{val}}
	}
	return nil
}

func parseHkRouter(out chan *Bucket, opts Options, header *lpx.Header, tups tuples) error {
	if string(header.Name) != routerPrefix {
		return nil
	}
	for i := range tups {
		id := new(Id)
		id.Resolution = parseResolution(opts)
		id.Time = parseTime(id.Resolution, header.Time)
		id.User = opts["user"][0]
		id.Pass = opts["password"][0]
		id.Source = tups.Source()
		id.Units = tups[i].Units()

		switch tups[i].Name() {
		case "bytes":
			id.Name = buildPrefix(opts, "router.bytes")
		case "connect":
			id.Name = buildPrefix(opts, "router.connect")
		case "service":
			id.Name = buildPrefix(opts, "router.service")
		}
		val, err := tups[i].Float64()
		if err != nil {
			continue
		}
		out <- &Bucket{Id: id, Vals: []float64{val}}
	}
	return nil
}

func buildPrefix(opts Options, suffix string) string {
	//Remove measure. from the name if present.
	if strings.HasPrefix(suffix, measurePrefix) {
		suffix = suffix[len(measurePrefix):]
	}
	pre, present := opts["prefix"]
	if !present {
		return suffix
	}
	return pre[0] + "." + suffix
}

func parseTime(d time.Duration, ts []byte) time.Time {
	t, err := time.Parse(time.RFC3339, string(ts))
	if err != nil {
		t = time.Now()
	}
	return time.Unix(0, int64((time.Duration(t.UnixNano())/d)*d))
}

func parseResolution(opts Options) time.Duration {
	resTmp, present := opts["resolution"]
	if !present {
		resTmp = []string{"60"}
	}

	res, err := strconv.Atoi(resTmp[0])
	if err != nil {
		return time.Minute
	}

	return time.Second * time.Duration(res)
}
