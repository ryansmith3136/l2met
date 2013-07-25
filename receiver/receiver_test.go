package receiver

import (
	"fmt"
	"l2met/bucket"
	"l2met/store"
	"testing"
	"time"
)

type testOps map[string][]string

func TestReceiver(t *testing.T) {
	currentTime := time.Now()
	opts := testOps{"resolution": []string{"60"}, "user": []string{"u"}, "password": []string{"p"}}
	cases := []struct {
		Name    string
		Opts    testOps
		LogLine []byte
		Buckets []*bucket.Bucket
	}{
		{
			"router",
			opts,
			fmtLog(currentTime, "router", "host=l2met.net connect=1ms service=4ms bytes=10"),
			[]*bucket.Bucket{
				testBucket("router.connect", "l2met.net", "u", "p", currentTime, time.Minute, []float64{1}),
				testBucket("router.service", "l2met.net", "u", "p", currentTime, time.Minute, []float64{4}),
				testBucket("router.bytes", "l2met.net", "u", "p", currentTime, time.Minute, []float64{10}),
			},
		},
		{
			"router large values",
			opts,
			fmtLog(currentTime, "router", "host=l2met.net connect=12345678912ms service=4ms bytes=12345678912"),
			[]*bucket.Bucket{
				testBucket("router.connect", "l2met.net", "u", "p", currentTime, time.Minute, []float64{12345678912}),
				testBucket("router.service", "l2met.net", "u", "p", currentTime, time.Minute, []float64{4}),
				testBucket("router.bytes", "l2met.net", "u", "p", currentTime, time.Minute, []float64{12345678912}),
			},
		},
		{
			"idiomatic",
			opts,
			fmtLog(currentTime, "app", "measure.a"),
			[]*bucket.Bucket{
				testBucket("a", "", "u", "p", currentTime, time.Minute, []float64{1}),
			},
		},
		{
			"change in resolution",
			opts,
			fmtLog(currentTime, "app", "measure.a"),
			[]*bucket.Bucket{
				testBucket("a", "", "u", "p", currentTime, time.Second, []float64{1}),
			},
		},
		{
			"with value",
			opts,
			fmtLog(currentTime, "app", "measure.a=1"),
			[]*bucket.Bucket{
				testBucket("a", "", "u", "p", currentTime, time.Minute, []float64{1}),
			},
		},
		{
			"with float value",
			opts,
			fmtLog(currentTime, "app", "measure.a=0.001"),
			[]*bucket.Bucket{
				testBucket("a", "", "u", "p", currentTime, time.Minute, []float64{0.001}),
			},
		},
		{
			"multiple values",
			opts,
			fmtLog(currentTime, "app", "measure.a=1 measure.b=2"),
			[]*bucket.Bucket{
				testBucket("a", "", "u", "p", currentTime, time.Minute, []float64{1}),
				testBucket("b", "", "u", "p", currentTime, time.Minute, []float64{2}),
			},
		},
	}

	for i := range cases {
		actual, err := receiveInput(cases[i].Opts, cases[i].LogLine)
		if err != nil {
			t.Errorf("error=%s\n", err)
		}
		expected := cases[i].Buckets
		if len(actual) != len(expected) {
			t.Fatalf("case=%s actual-len=%d expected-len=%d\n",
				cases[i].Name, len(actual), len(expected))
		}
		for j := range actual {
			if !bucketsEqual(actual[j], expected[j]) {
				t.Errorf("\n actual:\t %s \n expected:\t %s",
					actual[j].String(), expected[j].String())
			}
		}
	}
}

func testBucket(name, source, user, pass string, t time.Time, res time.Duration, vals []float64) *bucket.Bucket {
	id := new(bucket.Id)
	id.Name = name
	id.Source = source
	id.User = user
	id.Pass = pass
	id.Time = t.Truncate(res)
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
	st := store.NewMemStore()
	recv := NewReceiver(100, 1, time.Millisecond*5, st)
	recv.Start()
	defer recv.Stop()

	recv.Receive(msg, opts)
	time.Sleep(2 * recv.FlushInterval)

	future := time.Now().Add(time.Minute)
	ch, err := st.Scan(future)
	if err != nil {
		return nil, err
	}
	buckets := make([]*bucket.Bucket, 0)
	for b := range ch {
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
