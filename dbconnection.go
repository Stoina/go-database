package database

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"

	dbmodel "github.com/Stoina/go-database/model"
)

// DBConnection exported
// DBConnection ...
type DBConnection struct {
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
func OpenDBConnection(driverName string, host string, port int, user string, password string, database string) (*dbmodel.DBConnection, error) {

	dbConnection, err := dbmodel.NewDBConnection(driverName, host, port, user, password, database)

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
func NewDBConnection(driverName string, host string, port int, user string, password string, database string) (*DBConnection, error) {

	connectionString := getConnectionString(driverName, host, port, user, password, database)

	if connectionString != "" {
		return &DBConnection{
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
