package bucket

import (
	"testing"
)

func TestParseVal(t *testing.T) {
	expectedVal := float64(99)
	expectedUnits := "g"
	actualUnits, actualVal := parseVal("99g")

	if expectedVal != actualVal {
		t.Errorf("actualVal=%d expectedVal=%d\n", actualVal, expectedVal)
	}

	if expectedUnits != expectedUnits {
		t.Errorf("actualUnits=%d expectedUnits=%d\n", actualUnits, expectedUnits)
	}
}
