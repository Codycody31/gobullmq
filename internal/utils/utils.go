package utils

import (
	"crypto/md5"
	"encoding/hex"
)

func MD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

// Array2obj converts a flat slice received from Redis HGETALL (key, value, key, value...)
// into a map[string]any.
func Array2obj(raw any) map[string]any {
	obj := make(map[string]any)
	if rawArray, ok := raw.([]any); ok {
		// An odd number of elements is tolerated: pairs are processed as far
		// as they go, and a trailing key gets a nil value.
		for i := 0; i < len(rawArray); i += 2 {
			key, keyOk := rawArray[i].(string)
			if !keyOk {
				continue
			}
			if i+1 < len(rawArray) {
				obj[key] = rawArray[i+1]
			} else {
				obj[key] = nil
			}
		}
	}
	return obj
}
