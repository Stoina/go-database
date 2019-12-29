package database

import (
	"database/sql"

	dbmodel "github.com/Stoina/go-database/model"

	_ "github.com/lib/pq"
)

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
