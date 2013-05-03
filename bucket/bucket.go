package bucket

import (
	"math"
	"sort"
	"sync"
)

const DefaultUnit = "u"

type Bucket struct {
	// A bucket can be locked to ensure safe memory access.
	sync.Mutex
	// The identity of a bucket is used in registers and as keys in redis.
	Id *Id
	// A slice of all the measurements for a bucket.
	Vals []float64 `json:"vals,omitempty"`
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

/*
 The remainder of these methods provide statistics for buckets.
*/

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
