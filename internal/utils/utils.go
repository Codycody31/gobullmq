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
// into a map[string]interface{}.
func Array2obj(raw interface{}) map[string]interface{} {
	obj := make(map[string]interface{})
	if rawArray, ok := raw.([]interface{}); ok {
		// Ensure we have an even number of elements (key-value pairs)
		if len(rawArray)%2 != 0 {
			// fmt.Println("Warning: Array2obj received an array with an odd number of elements")
			// Handle this? Return nil? Try to process pairs anyway?
			// For now, attempt to process pairs.
		}
		// Iterate over key-value pairs
		for i := 0; i < len(rawArray); i += 2 {
			// The key should be a string
			key, keyOk := rawArray[i].(string)
			if !keyOk {
				// fmt.Printf("Warning: Array2obj expected string key at index %d, got %T\n", i, rawArray[i])
				continue // Skip this pair if the key isn't a string
			}
			// Ensure there's a value for this key
			if i+1 < len(rawArray) {
				obj[key] = rawArray[i+1]
			} else {
				// fmt.Printf("Warning: Array2obj found key '%s' without a corresponding value\n", key)
				// Assign nil or skip? Assigning nil for now.
				obj[key] = nil
			}
		}
	}
	return obj
}

