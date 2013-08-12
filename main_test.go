package main

import (
	"fmt"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/receiver"
	"github.com/ryandotsmith/l2met/store"
	"testing"
	"time"
)

type testOps map[string][]string

var currentTime = time.Now()

var opts = testOps{
	"resolution": []string{"60"},
	"auth":       []string{"abc123"},
}

var integrationTest = []struct {
	desc    string
	opts    testOps
	logLine []byte
	buckets []*bucket.Bucket
}{
	{
		"router",
		opts,
		fmtLog(currentTime, "router", "host=l2met.net connect=1ms service=4ms bytes=10"),
		[]*bucket.Bucket{
			build("router.connect", "", "u", currentTime, time.Minute, []float64{1}),
			build("router.service", "", "u", currentTime, time.Minute, []float64{4}),
			build("router.bytes", "", "u", currentTime, time.Minute, []float64{10}),
		},
	},
	{
		"router large values",
		opts,
		fmtLog(currentTime, "router", "host=l2met.net connect=12345678912ms service=4ms bytes=12345678912"),
		[]*bucket.Bucket{
			build("router.connect", "", "u", currentTime, time.Minute, []float64{12345678912}),
			build("router.service", "", "u", currentTime, time.Minute, []float64{4}),
			build("router.bytes", "", "u", currentTime, time.Minute, []float64{12345678912}),
		},
	},
	{
		"legacy",
		opts,
		fmtLog(currentTime, "app", "measure.a"),
		[]*bucket.Bucket{
			build("a", "", "u", currentTime, time.Minute, []float64{1}),
		},
	},
	{
		"idiomatic",
		opts,
		fmtLog(currentTime, "app", "measure#a"),
		[]*bucket.Bucket{
			build("a", "", "u", currentTime, time.Minute, []float64{1}),
		},
	},
	{
		"change in resolution",
		opts,
		fmtLog(currentTime, "app", "measure#a"),
		[]*bucket.Bucket{
			build("a", "", "u", currentTime, time.Second, []float64{1}),
		},
	},
	{
		"with value",
		opts,
		fmtLog(currentTime, "app", "measure#a=1"),
		[]*bucket.Bucket{
			build("a", "", "u", currentTime, time.Minute, []float64{1}),
		},
	},
	{
		"with float value",
		opts,
		fmtLog(currentTime, "app", "measure#a=0.001"),
		[]*bucket.Bucket{
			build("a", "", "u", currentTime, time.Minute, []float64{0.001}),
		},
	},
	{
		"multiple values",
		opts,
		fmtLog(currentTime, "app", "measure#a=1 measure#b=2"),
		[]*bucket.Bucket{
			build("a", "", "u", currentTime, time.Minute, []float64{1}),
			build("b", "", "u", currentTime, time.Minute, []float64{2}),
		},
	},
	{
		"counters",
		opts,
		fmtLog(currentTime, "app", "count#a=1"),
		[]*bucket.Bucket{
			build("a", "", "u", currentTime, time.Minute, []float64{1}),
		},
	},
	{
		"samples",
		opts,
		fmtLog(currentTime, "app", "sample#a=1"),
		[]*bucket.Bucket{
			build("a", "", "u", currentTime, time.Minute, []float64{1}),
		},
	},
	{
		"source prefix",
		testOps{"auth": []string{"abc123"}, "source-prefix": []string{"srcpre"}},
		fmtLog(currentTime, "app", "measure#hello"),
		[]*bucket.Bucket{
			build("hello", "srcpre", "u", currentTime, time.Minute, []float64{1}),
		},
	},
}

func TestReceiver(t *testing.T) {
	for _, ts := range integrationTest {
		actual, err := receiveInput(ts.opts, ts.logLine)
		if err != nil {
			t.Fatalf("error=%s\n", err)
		}
		expected := ts.buckets
		if len(actual) != len(expected) {
			t.Fatalf("case=%s actual-len=%d expected-len=%d\n",
				ts.desc, len(actual), len(expected))
		}
		for j := range actual {
			found := false
			for i := range expected {
				if bucketsEqual(actual[j], expected[i]) {
					found = true
				}
			}
			if !found {
				t.Fatalf("\n actual:\t %v \n expected:\t %v",
					actual[j], expected[j])
			}
		}
	}
}

func build(name, source, auth string, t time.Time, res time.Duration, vals []float64) *bucket.Bucket {
	id := new(bucket.Id)
	id.Name = name
	id.Source = source
	id.Auth = auth
	id.Time = t.Truncate(res)
	id.ReadyAt = t
	id.Resolution = res
	return &bucket.Bucket{Id: id, Vals: vals}
}

func fmtLog(t time.Time, procid, msg string) []byte {
	prival := 190 //local7/info
	version := 1
	timestamp := t.Format("2006-01-02T15:04:05+00:00")
	hostname := "hostname"
	appname := "app"
	msgid := "-"
	layout := "<%d>%d %s %s %s %s %s %s"
	packet := fmt.Sprintf(layout,
		prival, version, timestamp, hostname, appname, procid, msgid, msg)
	result := fmt.Sprintf("%d %s", len(packet), packet)
	return []byte(result)
}

func receiveInput(opts testOps, msg []byte) ([]*bucket.Bucket, error) {
	cfg := &conf.D{
		Concurrency:      1,
		BufferSize:       10,
		FlushInterval:    time.Millisecond * 5,
		ReceiverDeadline: 2,
		MaxPartitions:    1,
		RedisHost:        "localhost:6379",
	}
	st := store.NewRedisStore(cfg)
	st.Flush()
	st.Mchan = new(metchan.Channel)
	recv := receiver.NewReceiver(cfg, st)
	recv.Mchan = new(metchan.Channel)
	recv.Start()
	recv.Receive(msg, opts)
	recv.Wait()
	d, err := time.ParseDuration(opts["resolution"][0] + "s")
	if err != nil {
		return nil, err
	}
	schedule := currentTime.Add(d).Truncate(d)
	ch, err := st.Scan(schedule)
	if err != nil {
		return nil, err
	}
	var buckets []*bucket.Bucket
	for b := range ch {
		st.Get(b)
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func bucketsEqual(actual, expected *bucket.Bucket) bool {
	if actual.Id.Name != expected.Id.Name {
		return false
	}
	if actual.Id.Source != expected.Id.Source {
		return false
	}
	if actual.Sum() != expected.Sum() {
		return false
	}
	return true
}
