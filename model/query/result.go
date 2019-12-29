package dbmodel

import (
	"encoding/json"
)

// QueryResult exported
// QueryResult ...
type QueryResult struct {
	Data []map[string]interface{}
}

// ConvertToJSON exported
// ConvertToJSON ...
func (result *QueryResult) ConvertToJSON() (string, error) {

	jsonBytes, err := json.Marshal(result.Data)

	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}
