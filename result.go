package db

import (
	"encoding/json"
)

// Result exported
// Result ...
type Result struct {
	RowCount int
	Data     []map[string]interface{}
}

// ConvertDataToJSONString exported
// ...
func (result *Result) ConvertDataToJSONString() (string, error) {

	jsonBytes, err := json.Marshal(result.Data)

	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}
