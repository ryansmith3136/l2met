// An internal metrics channel.
// l2met internal components can publish their metrics
// here and they will be outletted to Librato.
package metchan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ryandotsmith/l2met/bucket"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Convert l2met data into Librato's API format.
type libratoMetric struct {
	Name   string  `json:"name"`
	Time   int64   `json:"measure_time"`
	Source string  `json:"source"`
	Count  int     `json:"count"`
	Sum    float64 `json:"sum"`
	Max    float64 `json:"max"`
	Min    float64 `json:"min"`
}

type libratoPayload struct {
	Gauges []*libratoMetric `json:"gauges"`
}

// The channel for which internal metrics are organized.
type Channel struct {
	sync.Mutex
	username      string
	password      string
	verbose       bool
	enabled       bool
	buffer        map[bucket.Id]*bucket.Bucket
	outbox        chan *bucket.Bucket
	url           *url.URL
	FlushInterval time.Duration
}

// Returns an initialized Metchan Channel.
// Creates a new HTTP client for direct access to Librato.
// This channel is orthogonal with other librato http clients in l2met.
// If a blank URL is given, no metric posting attempt will be made.
// If verbose is set to true, the metric will be printed to STDOUT
// regardless of whether the metric is sent to Librato.
func New(verbose bool, u *url.URL) *Channel {
	c := new(Channel)

	// Read the destination for the metrics
	c.url = u
	c.enabled = len(u.String()) > 0
	c.username = u.User.Username()
	c.password, _ = u.User.Password()
	c.url.User = nil

	// This will enable writting to a logger.
	c.verbose = verbose

	// Internal Datastructures.
	c.buffer = make(map[bucket.Id]*bucket.Bucket)
	c.outbox = make(chan *bucket.Bucket, 10)

	// Default flush interval.
	c.FlushInterval = time.Second

	return c
}

func (c *Channel) Start() {
	go c.scheduleFlush()
	go c.outlet()
}

// Provide the time at which you started your measurement.
// Places the measurement in a buffer to be aggregated and
// eventually flushed to Librato.
func (c *Channel) Measure(name string, t time.Time) {
	elapsed := time.Since(t) / time.Millisecond
	if c.verbose {
		fmt.Printf("measure.%s=%f\n", name, float64(elapsed))
	}
	if !c.enabled {
		return
	}
	b := &bucket.Bucket{
		Id: &bucket.Id{
			Time:       time.Now().Truncate(time.Minute),
			Resolution: time.Minute,
			Name:       name,
			Units:      "ms",
			// TODO(ryandotsmith):
			// Maybe we use the system's hostname?
			Source: "metchan",
		},
		Vals: []float64{float64(elapsed)},
	}
	c.add(b)
}

func (c *Channel) add(b *bucket.Bucket) {
	c.Lock()
	defer c.Unlock()
	existing, present := c.buffer[*b.Id]
	if !present {
		c.buffer[*b.Id] = b
		return
	}
	existing.Add(b)
}

func (c *Channel) scheduleFlush() {
	for _ = range time.Tick(c.FlushInterval) {
		c.flush()
	}
}

func (c *Channel) flush() {
	c.Lock()
	defer c.Unlock()
	for id, b := range c.buffer {
		c.outbox <- b
		delete(c.buffer, id)
	}
}

func (c *Channel) outlet() {
	for b := range c.outbox {
		met := &libratoMetric{
			Name:   b.Id.Name,
			Time:   b.Id.Time.Unix(),
			Source: b.Id.Source,
			Count:  b.Count(),
			Sum:    b.Sum(),
			Max:    b.Max(),
			Min:    b.Min(),
		}
		if err := c.post(met); err != nil {
			fmt.Printf("at=metchan-post error=%s\n", err)
		} else {
			fmt.Printf("at=metchan-post status=success\n")
		}
	}
}

func (c *Channel) post(m *libratoMetric) error {
	p := &libratoPayload{[]*libratoMetric{m}}
	j, err := json.Marshal(p)
	if err != nil {
		return err
	}
	body := bytes.NewBuffer(j)
	req, err := http.NewRequest("POST", c.url.String(), body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "l2met-metchan/0")
	req.SetBasicAuth(c.username, c.password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var m string
		s, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			m = fmt.Sprintf("code=%d", resp.StatusCode)
		} else {
			m = fmt.Sprintf("code=%d resp=body=%s req-body=%s",
				resp.StatusCode, s, body)
		}
		return errors.New(m)
	}
	return nil
}
