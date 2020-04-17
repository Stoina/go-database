package db

import (
	"fmt"
)

// InsertStatement exported
// ...
type InsertStatement struct {
	Columns []string
	Values  []interface{}
	Table   string
}

// NewInsertStatement exported
// ...
func NewInsertStatement(table string, columns []string, values []interface{}) *InsertStatement {
	return &InsertStatement{
		Table:   table,
		Columns: columns,
		Values:  values}
}

// ToString exported
// ...
func (stat *InsertStatement) ToString() string {

	columnsCount := len(stat.Columns)

	columnsSection := ""
	valuesSection := ""

	for i := 0; i < columnsCount; i++ {

		stringValue := fmt.Sprintf("%v", stat.Values[i])

		if i == 0 {
			columnsSection += "\"" + stat.Columns[i] + "\""
			valuesSection += "'" + stringValue + "'"
		} else {
			columnsSection += ", \"" + stat.Columns[i] + "\""
			valuesSection += ", '" + stringValue + "'"
		}

	}

	return "insert into \"" + stat.Table + "\" (" + columnsSection + ") values (" + valuesSection + ")"
}
