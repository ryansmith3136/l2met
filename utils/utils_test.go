package utils

import (
	"bytes"
	"net/http"
	"testing"
)

func TestParseAuthDBToken(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}
	r.SetBasicAuth("l2met", "token")
	expectedUser, expectedPass, err := ParseAuth(r)

	if err != nil {
		t.Error(err)
	}

	if expectedUser != "l2met" {
		t.Errorf("expected=%q actual=%q\n", "l2met", expectedUser)
	}

	if expectedPass != "token" {
		t.Errorf("expected=%q actual=%q\n", "token", expectedPass)
	}
}

func TestParseAuthNoPassword(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}
	_, _, err = ParseAuth(r)
	if err == nil {
		t.Errorf("Expected ParseAuth to return error when with no creds.")
	}
}
