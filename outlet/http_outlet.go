package outlet

import (
	"l2met/bucket"
	"l2met/store"
	"l2met/utils"
	"net/http"
	"time"
	"strconv"
)

type HttpOutlet struct {
	Store store.Store
}

type Metric struct {
	Name   string  `json:"name"`
	Mean   float64 `json:"mean"`
	Median float64 `json:"median"`
	Count  int     `json:"count"`
}

func (h *HttpOutlet) ServeReadBucket(w http.ResponseWriter, r *http.Request) {
	// need to extract: token, source, name, time
	// https://l2met:token@l2met.net/buckets/:name
	tok, err := utils.ParseToken(r)
	if err != nil {
		return
	}

	q := r.URL.Query()
	src := q.Get("source") // It is ok if src is blank.
	name := q.Get("name")
	if len(name) == 0 {
		http.Error(w, "Invalid Request. Name is required.", 400)
		return
	}
	limitStr := q.Get("limit")
	if len(limitStr) == 0{
		limitStr = "5"
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		http.Error(w, "Invalid Request. Limit must be an int.", 400)
		return
	}
	resolution := q.Get("resolution")
	if len(resolution) == 0 {
		resolution = "second"
	}

	t := time.Now()
	var metrics []*Metric
	for i := 0; i < limit; i++ {
		adjTime := h.adjustTime(resolution, t, i)
		id := &bucket.Id{Token: tok, Name: name, Source: src, Time: adjTime}
		b := &bucket.Bucket{Id: id}
		h.Store.Get(b)

		m := new(Metric)
		m.Name = b.Id.Name
		m.Mean = b.Mean()
		m.Median = b.Median()
		m.Count = b.Count()
		metrics = append(metrics, m)
	}
	utils.WriteJson(w, 200, metrics)
}

func (h *HttpOutlet) adjustTime(res string, t time.Time, i int) time.Time {
	switch res {
	case "second":
		return t.Add(-1 * time.Duration(i) * time.Second)
	default:
		return t.Add(-1 * time.Duration(i) * time.Minute)
	}
	panic("impossible")
}
