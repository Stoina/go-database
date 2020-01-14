package db

import (
	"encoding/json"
)

// Result exported
// Result ...
type Result struct {
	Data []map[string]interface{}
}

// ConvertToJSON exported
// ConvertToJSON ...
func (result *Result) ConvertToJSON() (string, error) {

	jsonBytes, err := json.Marshal(result.Data)

	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

