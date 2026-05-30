package queueid

import (
	"encoding/base64"
	"strings"
)

const prefix = "q:"

func Encode(instanceID, token string) string {
	if instanceID == "" {
		return token
	}

	return prefix + base64.RawURLEncoding.EncodeToString([]byte(instanceID)) + ":" + token
}

func Decode(deliveryID string) (instanceID, token string, ok bool) {
	if !strings.HasPrefix(deliveryID, prefix) {
		return "", deliveryID, false
	}

	rest := strings.TrimPrefix(deliveryID, prefix)
	encoded, token, found := strings.Cut(rest, ":")
	if !found {
		return "", deliveryID, false
	}

	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", deliveryID, false
	}

	instanceID = string(decoded)
	if instanceID == "" {
		return "", deliveryID, false
	}

	return instanceID, token, true
}
