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

type libratoGauge struct {
	Gauges []*libratoMetric `json:"gauges"`
}

// The channel for which internal metrics are organized.
type Channel struct {
	sync.Mutex
	username      string
	password      string
	verbose       bool
	enabled       bool
	buffer        map[string]*bucket.Bucket
	outbox        chan *libratoMetric
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

	// If the url is nil, then it wasn't initialized
	// by the conf pkg. If it is not nil, we will
	// enable the Metchan.
	if u != nil {
		c.url = u
		c.username = u.User.Username()
		c.password, _ = u.User.Password()
		c.url.User = nil
		c.enabled = true
	}

	// This will enable writting to a logger.
	c.verbose = verbose

	// Internal Datastructures.
	c.buffer = make(map[string]*bucket.Bucket)
	c.outbox = make(chan *libratoMetric, 10)

	// Default flush interval.
	c.FlushInterval = time.Minute

	return c
}

func (c *Channel) Start() {
	if c.enabled {
		go c.scheduleFlush()
		go c.outlet()
	}
}

// Provide the time at which you started your measurement.
// Places the measurement in a buffer to be aggregated and
// eventually flushed to Librato.
func (c *Channel) Measure(name string, t time.Time) {
	elapsed := float64(time.Since(t) / time.Millisecond)
	if c.verbose {
		fmt.Printf("measure.%s=%f\n", name, elapsed)
	}
	if !c.enabled {
		return
	}
	id := &bucket.Id{
		Resolution: c.FlushInterval,
		Name:       name,
		Units:      "ms",
		// TODO(ryandotsmith):
		// Maybe we use the system's hostname?
		Source: "metchan",
	}
	c.add(id, elapsed)
}

func (c *Channel) add(id *bucket.Id, val float64) {
	c.Lock()
	defer c.Unlock()
	b, ok := c.buffer[id.Name]
	if !ok {
		b = &bucket.Bucket{Id: id}
		b.Vals = make([]float64, 1, 10000)
		c.buffer[id.Name] = b
	}
	// Instead of creating a new bucket struct with a new Vals slice
	// We will re-use the old bucket and reset the slice. This
	// dramatically decreases the amount of arrays created and thus
	// led to better memory utilization.
	latest := time.Now().Truncate(c.FlushInterval)
	if b.Id.Time != latest {
		b.Id.Time = latest
		b.Vals = b.Vals[:0]
	}
	b.Vals = append(b.Vals, val)
}

func (c *Channel) scheduleFlush() {
	for _ = range time.Tick(c.FlushInterval) {
		c.flush()
	}
}

func (c *Channel) flush() {
	c.Lock()
	defer c.Unlock()
	for _, b := range c.buffer {
		c.outbox <- &libratoMetric{
			Name:   b.Id.Name,
			Time:   b.Id.Time.Unix(),
			Source: b.Id.Source,
			Count:  b.Count(),
			Sum:    b.Sum(),
			Max:    b.Max(),
			Min:    b.Min(),
		}
	}
}

func (c *Channel) outlet() {
	for met := range c.outbox {
		if err := c.post(met); err != nil {
			fmt.Printf("at=metchan-post error=%s\n", err)
		} else {
			fmt.Printf("at=metchan-post status=success\n")
		}
	}
}

func (c *Channel) post(m *libratoMetric) error {
	p := &libratoGauge{[]*libratoMetric{m}}
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
