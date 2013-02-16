package store

import (
	"l2met/encoding"
	"l2met/utils"
	"time"
)

type Metric struct {
	Time   time.Time `json:"time"`
	Name   string    `json:"name"`
	Source string    `json:"source,omitempty"`
	Mean   float64   `json:"mean"`
}

func GetMetrics(token, name string, resolution int64, min, max time.Time) ([]*Metric, error) {
  defer utils.MeasureT("get-metrics", time.Now())

	rows, err := pg.Query("select * from get_buckets($1, $2, $3, $4, $5)",
		token, name, resolution, min, max)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var metrics []*Metric
	for rows.Next() {
		var tmp []byte
    k := BKey{}
		rows.Scan(&k.Name, &k.Source, &k.Time, &tmp)
    b := Bucket{ Key: k }

		if len(tmp) == 0 {
			b.Vals = []float64{}
			continue
		}
		encoding.DecodeArray(tmp, &b.Vals, '{', '}', ',')
		m := new(Metric)
		m.Time = k.Time
		m.Name = k.Name
		m.Source = k.Source
		m.Mean = b.Mean()
		metrics = append(metrics, m)
	}

	return metrics, nil
}
