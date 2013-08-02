package bucket

import (
	"bytes"
	"testing"
	"time"
)

var idTest = []struct {
	id *Id
	name string
}{
	{
		&Id{Name: "hello world"},
		"hello world",
	},
}

func TestDecodeId(t *testing.T) {
	for _, ts := range idTest {
		b, err := ts.id.Encode()
		if err != nil {
			t.Error(err)
		}
		other := new(Id)
		other.Decode(bytes.NewBuffer(b))
		if other.Name != ts.name {
			t.Errorf("actual=%s\n", other.Name)
		}
	}
}

func TestDelayMinutes(t *testing.T) {
	base := time.Now()
	id := Id{Resolution: time.Minute, Time: base}
	actualDelay := id.Delay(base.Add(time.Minute * 2))
	expectedDelay := int64(2)

	if expectedDelay != actualDelay {
		t.Errorf("actual=%d expected=%d\n", actualDelay, expectedDelay)
	}
}

func TestDelaySeconds(t *testing.T) {
	base := time.Now()
	id := Id{Resolution: time.Second, Time: base}
	actualDelay := id.Delay(base.Add(time.Second * 2))
	expectedDelay := int64(2)

	if expectedDelay != actualDelay {
		t.Errorf("actual=%d expected=%d\n", actualDelay, expectedDelay)
	}
}

func TestDelayFiveSeconds(t *testing.T) {
	base := time.Now()
	id := Id{Resolution: (time.Second * 5), Time: base}
	actualDelay := id.Delay(base.Add(time.Second * 10))
	expectedDelay := int64(2)

	if expectedDelay != actualDelay {
		t.Errorf("actual=%d expected=%d\n", actualDelay, expectedDelay)
	}
}
