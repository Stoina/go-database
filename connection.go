package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"

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
func (dbConn *Connection) Query(query string) (*Result, error) {
	return executeQuery(query, dbConn.Database)
}

// Insert exported
// Insert new row and returns inserted row
func (dbConn *Connection) Insert(insertStatement *InsertStatement) (*Result, error) {
	return executeInsertStatement(insertStatement, dbConn)
}

// CallProcedure exported
// ...
func (dbConn *Connection) CallProcedure(procedureName string, parameter []interface{}) (*Result, error) {
	return executeProcedureCall(procedureName, parameter, dbConn)
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

func executeQuery(query string, db *sql.DB) (*Result, error) {

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

	rowCount := 0
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
		rowCount++
	}

	return &Result{RowCount: rowCount, Data: tableData}, nil
}

func executeInsertStatement(insertStatement *InsertStatement, dbConn *Connection) (*Result, error) {

	stringInsertStatement := insertStatement.ToString()

	log.Println("Execute new insert statement: " + stringInsertStatement)

	_, err := dbConn.Database.Exec(stringInsertStatement)

	if err != nil {
		return nil, err
	}

	idColumnName, err := readIDColumnFromTable(insertStatement.Table, dbConn)

	if err != nil {
		return nil, err
	}

	maxID, err := readMaxIDFromTable(insertStatement.Table, idColumnName, dbConn)

	if err != nil {
		return nil, err
	}

	insertedRowQuery := "select * from \"" + insertStatement.Table + "\" where \"" + idColumnName + "\" = " + strconv.Itoa(maxID)

	return executeQuery(insertedRowQuery, dbConn.Database)
}

func executeProcedureCall(procedureName string, parameter []interface{}, dbConn *Connection) (*Result, error) {

	parameterCount := len(parameter)
	parameterString := ""

	for i := 0; i < parameterCount; i++ {
		parameterValue := fmt.Sprintf("%v", parameter[i])

		if i < parameterCount-1 {
			parameterString += "'" + parameterValue + "', "
		} else {
			parameterString += "'" + parameterValue + "'"
		}

	}

	procedureCall := "call " + procedureName + "(" + parameterString + ")"

	res, err := dbConn.Database.Exec(procedureCall)

	if err != nil {
		return nil, err
	}

	fmt.Println(res)

	return nil, nil
}

func readIDColumnFromTable(tableName string, dbConn *Connection) (string, error) {
	columnNames, err := ReadColumnNamesFromTable(dbConn, tableName)

	if err != nil {
		return "", err
	}

	return columnNames[0], nil
}

func readMaxIDFromTable(tableName string, idColumnName string, dbConn *Connection) (int, error) {
	var maxID int

	maxIDRow := dbConn.Database.QueryRow("select max(\"" + idColumnName + "\") from \"" + tableName + "\"")
	err := maxIDRow.Scan(&maxID)

	if err != nil {
		return -1, err
	}

	return maxID, nil
}
