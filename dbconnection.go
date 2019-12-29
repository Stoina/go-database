package db

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"

	dbmodel "github.com/Stoina/go-database/model/query"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"
)

// Connection exported
// Connection ...
type Connection struct {
	DriverName   string
	Host         string
	Port         int
	User         string
	Password     string
	DatabaseName string

	ConnectionString string

	Database *sql.DB
}

// OpenDBConnection exported
// OpenDBConnection ...
func OpenDBConnection(driverName string, host string, port int, user string, password string, database string) (*Connection, error) {

	dbConnection, err := NewDBConnection(driverName, host, port, user, password, database)

	if err != nil {
		return nil, err
	}

	db, err := sql.Open(dbConnection.DriverName, dbConnection.ConnectionString)

	if err != nil {
		return nil, err
	}

	dbConnection.Database = db

	return dbConnection, nil
}

// NewDBConnection exported
// NewDBConnection ...
func NewDBConnection(driverName string, host string, port int, user string, password string, database string) (*Connection, error) {

	connectionString := getConnectionString(driverName, host, port, user, password, database)

	if connectionString != "" {
		return &Connection{
			DriverName:   driverName,
			Host:         host,
			Port:         port,
			User:         user,
			Password:     password,
			DatabaseName: database,

			ConnectionString: connectionString,

			Database: nil}, nil
	}

	errorText := "No known database driver name given: " + driverName

	return nil, errors.New(errorText)
}

// Query exported
// Query ...
// Returns selected rows as json string
func (dbConn *Connection) Query(query string) (*dbmodel.QueryResult, error) {
	return executeQuery(query, dbConn.Database)
}

func getConnectionString(driverName string, host string, port int, user string, password string, database string) string {

	switch driverName {
	case "postgres":
		return getPostgresSQLConnectionString(host, port, user, password, database)
	case "sqlserver":
		return getMSSQLConnectionString(host, port, user, password, database)
	}

	return ""
}

func getPostgresSQLConnectionString(host string, port int, user string, password string, database string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, database)
}

func getMSSQLConnectionString(host string, port int, user string, password string, database string) string {

	u := url.URL{
		Scheme: "sqlserver",
		Host:   fmt.Sprintf("%s:%d", host, port),
		User:   url.UserPassword(user, password),
	}

	return u.String()
}

func executeQuery(query string, db *sql.DB) (*dbmodel.QueryResult, error) {
	rows, err := db.Query(query)

	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	columnCount := len(columns)

	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, columnCount)
	valuePtrs := make([]interface{}, columnCount)

	for rows.Next() {

		for i := 0; i < columnCount; i++ {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		entry := make(map[string]interface{})

		for i, col := range columns {

			var v interface{}
			val := values[i]

			b, ok := val.([]byte)

			if ok {
				v = string(b)
			} else {
				v = val
			}

			entry[col] = v
		}

		tableData = append(tableData, entry)
	}

	return &dbmodel.QueryResult{Data: tableData}, nil
}
