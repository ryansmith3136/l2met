package auth

import (
	"encoding/base64"
	"errors"
	"github.com/kr/fernet"
	"l2met/conf"
	"net/http"
	"strings"
	"time"
)

var (
	OneHundredYears = time.Hour * 24 * 365 * 100
	keys            []*fernet.Key
)

func init() {
	if len(conf.Secrets) > 0 {
		keys = fernet.MustDecodeKeys(conf.Secrets...)
	}
}

func Sign(b []byte) ([]byte, error) {
	for i := range keys {
		if res, err := fernet.EncryptAndSign(b, keys[i]); err == nil {
			return res, err
		}
	}
	return []byte(""), errors.New("Unable to sign payload.")
}

func parseAuthHeader(r *http.Request) (string, error) {
	header, ok := r.Header["Authorization"]
	if !ok {
		return "", errors.New("Unable to parse Authorization header.")
	}
	return header[0], nil
}

func ParseRaw(header string) (string, string, error) {
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 {
		return "", "", errors.New("Authorization header malformed.")
	}

	method := parts[0]
	if method != "Basic" {
		return "", "", errors.New("Authorization must be basic.")
	}

	payload := parts[1]
	decodedPayload, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", "", err
	}

	userPass := strings.SplitN(string(decodedPayload), ":", 2)
	switch len(userPass) {
	case 1:
		return userPass[0], "", nil
	case 2:
		return userPass[0], userPass[1], nil
	}

	return "", "", errors.New("Unable to parse username or password.")
}

func Parse(r *http.Request) (string, string, error) {
	header, err := parseAuthHeader(r)
	if err != nil {
		return "", "", err
	}

	user, _, err := ParseRaw(header)
	if err != nil {
		return "", "", err
	}

	//If we have gotten here, we have a signed, db-less authentication reque
	//If we can verify and decrypt, then we will pass the decrypted credenti
	//to the caller. Most of the time, the username and password will be
	//credentials to outlet providers. (e.g. Librato creds or Graphite creds
	//We care about the validity of those credentials here. If they are wron
	//the metrics will be dropped at the outlet. Keep an eye on http
	//authentication errors from the log output of the outlets.
	if len(keys) > 0 {
		if s := fernet.VerifyAndDecrypt([]byte(user), OneHundredYears, keys); s != nil {
			parts := strings.Split(string(s), ":")
			return parts[0], parts[1], nil
		}
	}
	return "", "", errors.New("End of Authentication chain reached.")
}
