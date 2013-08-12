// Provides helpers for authentication & authorization via HTTP.
// Outlet credentials are encrypted and signed in the authorization payload.
package auth

import (
	"encoding/base64"
	"errors"
	"github.com/kr/fernet"
	"os"
	"strings"
	"time"
)

var (
	ttl  = time.Hour * 24 * 365 * 100
	keys []*fernet.Key
)

func init() {
	s := os.Getenv("SECRETS")
	if len(s) > 0 {
		keys = fernet.MustDecodeKeys(strings.Split(s, ":")...)
	}
}

// Use the first valid key to sign b.
// Returns error if no key is able to sign b.
func EncryptAndSign(b []byte) ([]byte, error) {
	for i := range keys {
		if res, err := fernet.EncryptAndSign(b, keys[i]); err == nil {
			return res, err
		}
	}
	return []byte(""), errors.New("Unable to sign payload.")
}

// Extract the username and password from the authorization
// line of an HTTP header. This function will handle the
// parsing and decoding of the line.
func Parse(authLine string) (string, error) {
	parts := strings.SplitN(authLine, " ", 2)
	if len(parts) != 2 {
		return "", errors.New("Authorization header malformed.")
	}
	method := parts[0]
	if method != "Basic" {
		return "", errors.New("Authorization must be basic.")
	}
	payload := parts[1]
	decodedPayload, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", err
	}
	return string(decodedPayload), nil
}

func Decrypt(s string) (string, string, error) {
	msg := fernet.VerifyAndDecrypt([]byte(s), ttl, keys)
	if msg == nil {
		return "", "", errors.New("Unable to decrypt.")
	}
	parts := strings.Split(string(msg), ":")
	return parts[0], parts[1], nil
}
