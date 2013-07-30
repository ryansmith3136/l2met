package metchan

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func serve(a *[]byte) (*url.URL, *httptest.Server) {
	f := func(w http.ResponseWriter, r *http.Request) {
		*a, _ = ioutil.ReadAll(r.Body)
	}
	srv := httptest.NewServer(http.HandlerFunc(f))
	u, _ := url.Parse(srv.URL)
	u.User = url.UserPassword("", "")
	return u, srv
}

var metTests = []struct {
	name  string
	start time.Time
}{

	{
		"simple.test",
		time.Now(),
	},
}

func TestMetchan(t *testing.T) {
	for _, ts := range metTests {
		var actual []byte
		u, srv := serve(&actual)
		mchan := New(false, u)
		mchan.FlushInterval = time.Millisecond * 500
		mchan.Start()
		mchan.Measure(ts.name, ts.start)
		time.Sleep(mchan.FlushInterval * 2)
		srv.Close()
		p := new(libratoPayload)
		if err := json.Unmarshal(actual, &p); err != nil {
			t.Error(err)
		}
		if p.Gauges[0].Name != ts.name {
			t.Errorf("actual=%s expected=%s\n",
				p.Gauges[0].Name, ts.name)
		}
	}
}
