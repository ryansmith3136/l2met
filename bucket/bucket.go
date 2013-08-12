// A collection of measurements.
package bucket

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

type libratoAttrs struct {
	Min   int    `json:"display_min"`
	Units string `json:"display_units_long"`
}

// When submitting data to Librato, we need to coerce
// our bucket representation into something their API
// can handle. Because there is not a 1-1 parity
// with the statistical functions that a bucket offers and
// the types of data the Librato API accepts (e.g. Librato does-
// not have support for p50, p95, p99) we need to expand
// our bucket into a set of LibratoMetric(s).
type LibratoMetric struct {
	Name   string        `json:"name"`
	Time   int64         `json:"measure_time"`
	Val    float64       `json:"value"`
	Source string        `json:"source,omitempty"`
	Auth   string        `json:"-"`
	Attr   *libratoAttrs `json:"attributes,omitempty"`
}

type Bucket struct {
	sync.Mutex
	Id   *Id
	Vals []float64
}

// Adding bucket a to bucket b copies the vals of bucket b and
// appends them to the vals of bucket a. Nothing is done with the
// Ids of the buckets. You should ensure that buckets have the same Id.
func (b *Bucket) Add(otherM *Bucket) {
	b.Lock()
	defer b.Unlock()
	for _, v := range otherM.Vals {
		b.Vals = append(b.Vals, v)
	}
}

// Relies on the Emitter to determine which type of
// metrics should be returned.
func (b *Bucket) Metrics() []*LibratoMetric {
	switch b.Id.Type {
	case "measurement":
		return b.EmitMeasurements()
	case "counter":
		return b.EmitCounters()
	case "sample":
		return b.EmiteSamples()
	default:
		panic("Undefined bucket.Id type.")
	}
}

// The standard emitter. All log data with `measure.foo` will
// be mapped to the MeasureEmitter.
func (b *Bucket) EmitMeasurements() []*LibratoMetric {
	metrics := make([]*LibratoMetric, 9)
	metrics[0] = b.Metric("min", b.Min())
	metrics[1] = b.Metric("median", b.Median())
	metrics[2] = b.Metric("p95", b.P95())
	metrics[3] = b.Metric("p99", b.P99())
	metrics[4] = b.Metric("max", b.Max())
	metrics[5] = b.Metric("mean", b.Mean())
	metrics[6] = b.Metric("sum", b.Sum())
	metrics[7] = b.Metric("count", float64(b.Count()))
	metrics[8] = b.Metric("last", b.Last())
	return metrics
}

func (b *Bucket) EmitCounters() []*LibratoMetric {
	metrics := make([]*LibratoMetric, 1)
	metrics[0] = b.Metric("sum", b.Sum())
	return metrics
}

func (b *Bucket) EmiteSamples() []*LibratoMetric {
	metrics := make([]*LibratoMetric, 1)
	metrics[0] = b.Metric("last", b.Last())
	return metrics
}

func (b *Bucket) Metric(name string, val float64) *LibratoMetric {
	return &LibratoMetric{
		Attr: &libratoAttrs{
			Min:   0,
			Units: b.Id.Units,
		},
		Name:   b.Id.Name + "." + name,
		Source: b.Id.Source,
		Time:   b.Id.Time.Unix(),
		Auth:   b.Id.Auth,
		Val:    val,
	}
}

func (b *Bucket) String() string {
	return fmt.Sprintf("name=%s source=%s vals=%v",
		b.Id.Name, b.Id.Source, b.Vals)
}

func (b *Bucket) Count() int {
	return len(b.Vals)
}

func (b *Bucket) Sum() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	s := float64(0)
	for i := range b.Vals {
		s += b.Vals[i]
	}
	return s
}

func (b *Bucket) Mean() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	return b.Sum() / float64(b.Count())
}

func (b *Bucket) Sort() {
	if !sort.Float64sAreSorted(b.Vals) {
		sort.Float64s(b.Vals)
	}
}

func (b *Bucket) Min() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	return b.Vals[0]
}

func (b *Bucket) Median() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Ceil(float64(b.Count() / 2)))
	return b.Vals[pos]
}

func (b *Bucket) P95() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.95))
	return b.Vals[pos]
}

func (b *Bucket) P99() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.99))
	return b.Vals[pos]
}

func (b *Bucket) Max() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := b.Count() - 1
	return b.Vals[pos]
}

func (b *Bucket) Last() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	pos := b.Count() - 1
	return b.Vals[pos]
}
