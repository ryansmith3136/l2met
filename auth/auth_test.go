package auth

import (
	"bytes"
	"encoding/base64"
	"github.com/kr/fernet"
	"net/http"
	"testing"
)

func TestAuth(t *testing.T) {
	if len(keys) == 0 {
		t.Fatalf("Must set $SECRETS\n")
	}

	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}

	expectedUser := "ryan@heroku.com"
	expectedPass := "abc123"
	tok, err := fernet.EncryptAndSign([]byte(expectedUser+":"+expectedPass),
		keys[0])
	if err != nil {
		t.Error(err)
	}
	r.Header.Set("Authorization",
		"Basic "+base64.StdEncoding.EncodeToString(tok))

	parseRes, err := Parse(r.Header["Authorization"][0])
	if err != nil {
		t.Fatalf("error=%s\n", err)
	}

	actualUser, actualPass, err := Decrypt(parseRes)
	if err != nil {
		t.Fatalf("error=%s\n", err)
	}

	if actualUser != expectedUser {
		t.Fatalf("actual=%q expected=%q\n", actualUser, expectedUser)
	}

	if actualPass != expectedPass {
		t.Fatalf("actual=%q expected=%q\n", actualPass, expectedPass)
	}
}
