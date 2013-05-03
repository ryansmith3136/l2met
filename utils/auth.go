package utils

import (
	"encoding/base64"
	"errors"
	"github.com/kr/fernet"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	OneHundredYears = time.Hour * 24 * 365 * 100
	keys            []*fernet.Key
)

func init() {
	if s := strings.Split(os.Getenv("SECRETS"), ":"); len(s[0]) > 0 {
		keys = fernet.MustDecodeKeys(s...)
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

func parseAuthValue(header string) (string, string, error) {
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

	userPass := strings.SplitN(string(decodedPayload), ":", 1)
	switch len(userPass) {
	case 1:
		return userPass[0], "", nil
	case 2:
		return userPass[0], userPass[1], nil
	}

	return "", "", errors.New("Unable to parse username or password.")
}

func ParseAuth(r *http.Request) (string, string, error) {
	header, err := parseAuthHeader(r)
	if err != nil {
		return "", "", err
	}

	user, _, err := parseAuthValue(header)
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
	//If the user is not == "l2met" then dbless-auth is requested.
	//ATM we assume the first part (user field) contains a base64 encoded
	//representation of the outlet credentials.
	if len(user) > 0 {
		trimmedUser := strings.Replace(user, ":", "", -1)
		decodedUser, err := base64.StdEncoding.DecodeString(trimmedUser)
		if err != nil {
			return trimmedUser, "", err
		}
		outletCreds := strings.Split(string(decodedUser), ":")
		//If the : is absent in parts[0], outletCreds[0] will contain the entire string in parts[0].
		u := outletCreds[0]
		//It is not required for the outletCreds to contain a pass.
		//The empty string that is returned will be handled by the outlet.
		var p string
		if len(outletCreds) > 1 {
			p = outletCreds[1]
		}
		return u, p, nil
	}
	return "", "", errors.New("End of Authe chain reached.")
}
