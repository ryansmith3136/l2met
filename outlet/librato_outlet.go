package outlet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"l2met/bucket"
	"l2met/token"
	"l2met/utils"
	"net/http"
	"strconv"
	"time"
)

var libratoUrl = "https://metrics-api.librato.com/v1/metrics"

type LibratoPayload struct {
	Name   string `json:"name"`
	Time   int64  `json:"measure_time"`
	Val    string `json:"value"`
	Source string `json:"source,omitempty"`
	Token  string `json:",omitempty"`
}

type LibratoRequest struct {
	Gauges []*LibratoPayload `json:"gauges"`
}

type LibratoOutlet struct {
	// The inbox is used to hold empty buckets that are
	// waiting to be processed. We buffer the chanel so
	// as not to slow down the fetch routine.
	Inbox chan *bucket.Bucket
	// The converter will take items from the inbox,
	// fill in the bucket with the vals, then convert the
	// bucket into a librato metric.
	Conversions chan *LibratoPayload
	// The converter will place the librato metrics into
	// the outbox for HTTP submission. We rely on the batch
	// routine to make sure that the collections of librato metrics
	// in the outbox are homogeneous with respect to their token.
	// This ensures that we route the metrics to the correct librato account.
	Outbox chan []*LibratoPayload
	// How many outlet routines should be running.
	NumOutlets int
	// How many accept routines should be running.
	NumConverters int
	Reader        Reader
	Retries       int
	User          string
	Pass          string
}

func NewLibratoOutlet() *LibratoOutlet {
	l := new(LibratoOutlet)
	l.Inbox = make(chan *bucket.Bucket, 10000)
	l.Conversions = make(chan *LibratoPayload, 10000)
	l.Outbox = make(chan []*LibratoPayload, 10000)
	return l
}

func (l *LibratoOutlet) Start() {
	go l.Reader.Start(l.Inbox)
	for i := 0; i < l.NumConverters; i++ {
		go l.convert()
	}
	go l.batch()
	for i := 0; i < l.NumOutlets; i++ {
		go l.outlet()
	}
}

func (l *LibratoOutlet) convert() {
	for bucket := range l.Inbox {
		if len(bucket.Vals) == 0 {
			fmt.Printf("at=bucket-no-vals bucket=%s\n", bucket.Id.Name)
			continue
		}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".last", Val: ff(bucket.Last())}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".min", Val: ff(bucket.Min())}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".max", Val: ff(bucket.Max())}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".mean", Val: ff(bucket.Mean())}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".median", Val: ff(bucket.Median())}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".perc95", Val: ff(bucket.P95())}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".perc99", Val: ff(bucket.P99())}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".count", Val: fi(bucket.Count())}
		l.Conversions <- &LibratoPayload{Token: bucket.Id.Token, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".sum", Val: ff(bucket.Sum())}
		delay := time.Now().Unix() - bucket.Id.Time.Unix()
		fmt.Printf("measure.bucket.conversion.delay=%d\n", delay)
	}
}

func (l *LibratoOutlet) batch() {
	ticker := time.Tick(time.Millisecond * 200)
	batchMap := make(map[string][]*LibratoPayload)
	for {
		select {
		case <-ticker:
			for k, v := range batchMap {
				if len(v) > 0 {
					l.Outbox <- v
				}
				delete(batchMap, k)
			}
		case payload := <-l.Conversions:
			_, present := batchMap[payload.Token]
			if !present {
				batchMap[payload.Token] = make([]*LibratoPayload, 1, 300)
				batchMap[payload.Token][0] = payload
			} else {
				batchMap[payload.Token] = append(batchMap[payload.Token], payload)
			}
			if len(batchMap[payload.Token]) == cap(batchMap[payload.Token]) {
				l.Outbox <- batchMap[payload.Token]
				delete(batchMap, payload.Token)
			}
		}
	}
}

func (l *LibratoOutlet) outlet() {
	for payloads := range l.Outbox {
		if len(payloads) < 1 {
			fmt.Printf("at=%q\n", "empty-metrics-error")
			continue
		}

		sample := payloads[0]
		tok := &token.Token{Id: sample.Token}

		// If a global user/token is provided, use the token for all metrics.
		// This enable a databaseless librato_outlet.
		if len(l.User) == 0 || len(l.Pass) == 0 {
			tok.Get()
		} else {
			tok.User = l.User
			tok.Pass = l.Pass
		}

		reqBody := new(LibratoRequest)
		reqBody.Gauges = payloads
		j, err := json.Marshal(reqBody)
		if err != nil {
			fmt.Printf("at=json-marshal-error error=%s\n", err)
			continue
		}

		l.postWithRetry(tok, bytes.NewBuffer(j))
	}
}

func (l *LibratoOutlet) postWithRetry(tok *token.Token, body *bytes.Buffer) error {
	for i := 0; i <= l.Retries; i++ {
		if err := l.post(tok, body); err != nil {
			fmt.Printf("error=%s attempt=%d\n", err, i)
			continue
			if i == l.Retries {
				return err
			}
		}
		return nil
	}
	panic("impossible")
}

func (l *LibratoOutlet) post(tok *token.Token, body *bytes.Buffer) error {
	defer utils.MeasureT("librato-post", time.Now())
	req, err := http.NewRequest("POST", libratoUrl, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(tok.User, tok.Pass)

	resp, err := http.DefaultClient.Do(req)
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

func ff(x float64) string {
	return strconv.FormatFloat(x, 'f', 5, 64)
}

func fi(x int) string {
	return strconv.FormatInt(int64(x), 10)
}

func ft(t time.Time) int64 {
	return t.Unix() + 59
}
