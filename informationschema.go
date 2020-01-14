package db

import (
	"database/sql"
)

// ReadColumnNamesFromTable exported
// ...
func ReadColumnNamesFromTable(dbConn *Connection, tableName string) ([]string, error) {

	if dbConn.DriverName == "postgres" {
		return readColumnNamesFromTableFromPostgres(dbConn.Database, tableName)
	}

	return nil, nil
}

func readColumnNamesFromTableFromPostgres(db *sql.DB, tableName string) ([]string, error) {
	postgresQuery := "SELECT column_name from information_schema.columns WHERE table_name = '" + tableName + "' ORDER BY ordinal_position;"
	return readColumnNames(db, postgresQuery)
}

func readColumnNames(db *sql.DB, query string) ([]string, error) {
	rows, err := db.Query(query)

	if err != nil {
		return nil, err
	}

	columnNames := make([]string, 0)
	
	for rows.Next() {
		columnName := ""
		rows.Scan(&columnName)

		columnNames = append(columnNames, columnName)
	}
	
	return columnNames, nil
}