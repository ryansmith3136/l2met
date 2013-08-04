package store

import (
	"github.com/ryandotsmith/l2met/bucket"
	"os"
	"testing"
)

func TestRedisGet(t *testing.T) {
	st := NewRedisStore(os.Getenv("REDIS_URL"), "", 1)
	st.flush()
	id := &bucket.Id{Name: "test"}
	b1 := &bucket.Bucket{
		Id:   id,
		Vals: []float64{99.99999, 1, 0.2},
	}
	st.Put(b1)
	b2 := &bucket.Bucket{Id: id}
	if err := st.Get(b2); err != nil {
		t.Error(err)
	}
	if len(b2.Vals) != len(b1.Vals) {
		t.Error("Expected size of b1 & b2 to be equal.")
		t.FailNow()
	}
	for i := range b1.Vals {
		if b1.Vals[i] != b2.Vals[i] {
			t.Errorf("b1[%d]= %f and b2[%d] = %f",
				i, b1.Vals[i], i, b2.Vals[i])
		}
	}
}
