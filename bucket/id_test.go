package bucket

import (
	"fmt"
	"testing"
	"time"
)

func TestParseId(t *testing.T) {
	id := Id{time.Now(), time.Minute, "user", "pass", "name", "units", "source"}
	expected, err := ParseId(id.String())
	if err != nil {
		t.FailNow()
	}
	if expected.Source != "source" {
		fmt.Printf("expected=%d actual=%d\n", "source", expected.Source)
		t.FailNow()
	}
	if expected.Name != "name" {
		fmt.Printf("expected=%d actual=%d\n", "name", expected.Name)
		t.FailNow()
	}
	if expected.User != "user" {
		fmt.Printf("expected=%d actual=%d\n", "user", expected.User)
		t.FailNow()
	}
	if expected.Pass != "pass" {
		fmt.Printf("expected=%d actual=%d\n", "pass", expected.Pass)
		t.FailNow()
	}
	if expected.Resolution != time.Minute {
		fmt.Printf("expected=%d actual=%d\n", time.Minute, expected.Resolution)
		t.FailNow()
	}
}

func TestParseIdWithoutSource(t *testing.T) {
	id := Id{time.Now(), time.Minute, "user", "pass", "name", "units", ""}
	expected, err := ParseId(id.String())
	if err != nil {
		t.FailNow()
	}
	if expected.Name != "name" {
		fmt.Printf("expected=%d actual=%d\n", "name", expected.Name)
		t.FailNow()
	}
	if expected.User != "user" {
		fmt.Printf("expected=%d actual=%d\n", "user", expected.User)
		t.FailNow()
	}
	if expected.Pass != "pass" {
		fmt.Printf("expected=%d actual=%d\n", "pass", expected.Pass)
		t.FailNow()
	}
	if expected.Resolution != time.Minute {
		fmt.Printf("expected=%d actual=%d\n", time.Minute, expected.Resolution)
		t.FailNow()
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
