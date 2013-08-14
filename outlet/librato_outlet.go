// The outlet pkg is responsible for taking
// buckets from the reader, formatting them in the Librato format
// and delivering the formatted librato metrics to Librato's API.
package outlet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ryandotsmith/l2met/auth"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/reader"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"time"
)

var libratoUrl = "https://metrics-api.librato.com/v1/metrics"

type libratoRequest struct {
	Gauges []*bucket.LibratoMetric `json:"gauges"`
}

type LibratoOutlet struct {
	inbox       chan *bucket.Bucket
	conversions chan *bucket.LibratoMetric
	outbox      chan []*bucket.LibratoMetric
	numOutlets  int
	rdr         *reader.Reader
	conn        *http.Client
	numRetries  int
	Mchan       *metchan.Channel
}

func buildClient(ttl time.Duration) *http.Client {
	tr := &http.Transport{
		DisableKeepAlives: true,
		Dial: func(n, a string) (net.Conn, error) {
			c, err := net.DialTimeout(n, a, ttl)
			if err != nil {
				return c, err
			}
			return c, c.SetDeadline(time.Now().Add(ttl))
		},
	}
	return &http.Client{Transport: tr}
}

func NewLibratoOutlet(cfg *conf.D, r *reader.Reader) *LibratoOutlet {
	l := new(LibratoOutlet)
	l.conn = buildClient(cfg.OutletTtl)
	l.inbox = make(chan *bucket.Bucket, cfg.BufferSize)
	l.conversions = make(chan *bucket.LibratoMetric, cfg.BufferSize)
	l.outbox = make(chan []*bucket.LibratoMetric, cfg.BufferSize)
	l.numOutlets = cfg.Concurrency
	l.numRetries = cfg.OutletRetries
	l.rdr = r
	return l
}

func (l *LibratoOutlet) Start() {
	go l.rdr.Start(l.inbox)
	// Converting is CPU bound as it reads from memory
	// then computes statistical functions over an array.
	for i := 0; i < runtime.NumCPU(); i++ {
		go l.convert()
	}
	go l.groupByUser()
	for i := 0; i < l.numOutlets; i++ {
		go l.outlet()
	}
}

func (l *LibratoOutlet) convert() {
	for bucket := range l.inbox {
		for _, m := range bucket.Metrics() {
			l.conversions <- m
		}
		delay := bucket.Id.Delay(time.Now())
		l.Mchan.Measure("outlet.delay", float64(delay))
	}
}

func (l *LibratoOutlet) groupByUser() {
	ticker := time.Tick(time.Millisecond * 200)
	m := make(map[string][]*bucket.LibratoMetric)
	for {
		select {
		case <-ticker:
			for k, v := range m {
				if len(v) > 0 {
					l.outbox <- v
				}
				delete(m, k)
			}
		case payload := <-l.conversions:
			usr := payload.Auth
			if _, present := m[usr]; !present {
				m[usr] = make([]*bucket.LibratoMetric, 1, 300)
				m[usr][0] = payload
			} else {
				m[usr] = append(m[usr], payload)
			}
			if len(m[usr]) == cap(m[usr]) {
				l.outbox <- m[usr]
				delete(m, usr)
			}
		}
	}
}

func (l *LibratoOutlet) outlet() {
	for payloads := range l.outbox {
		if len(payloads) < 1 {
			fmt.Printf("at=%q\n", "empty-metrics-error")
			continue
		}
		//Since a playload contains all metrics for
		//a unique librato user/pass, we can extract the user/pass
		//from any one of the payloads.
		user, pass, err := auth.Decrypt(payloads[0].Auth)
		if err != nil {
			fmt.Printf("error=%s\n", err)
			continue
		}
		libratoReq := &libratoRequest{payloads}
		j, err := json.Marshal(libratoReq)
		if err != nil {
			fmt.Printf("at=json error=%s user=%s\n", err, user)
			continue
		}
		if err := l.postWithRetry(user, pass, j); err != nil {
			l.Mchan.Measure("outlet.drop", 1)
		}
	}
}

func (l *LibratoOutlet) postWithRetry(u, p string, body []byte) error {
	for i := 0; i <= l.numRetries; i++ {
		if err := l.post(u, p, body); err != nil {
			fmt.Printf("measure.librato.error user=%s msg=%s attempt=%d\n", u, err, i)
			if i == l.numRetries {
				return err
			}
			continue
		}
		return nil
	}
	//Should not be possible.
	return errors.New("Unable to post.")
}

func (l *LibratoOutlet) post(u, p string, body []byte) error {
	defer l.Mchan.Time("outlet.post", time.Now())
	b := bytes.NewBuffer(body)
	req, err := http.NewRequest("POST", libratoUrl, b)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "l2met/"+conf.Version)
	req.SetBasicAuth(u, p)
	resp, err := l.conn.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var m string
		s, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			m = fmt.Sprintf("error=failed-request code=%d", resp.StatusCode)
		} else {
			m = fmt.Sprintf("error=failed-request code=%d resp=body=%s req-body=%s",
				resp.StatusCode, s, body)
		}
		return errors.New(m)
	}
	return nil
}
