package main

import (
	"fmt"
	"os"
	"strconv"

	db "github.com/Stoina/go-database"
)

func main() {

	driverName := os.Args[1]
	host := os.Args[2]
	port, err := strconv.Atoi(os.Args[3])
	user := os.Args[4]
	password := os.Args[5]
	databaseName := os.Args[6]

	if err != nil {
		fmt.Println("Can't cast port to an integer")
	}

	fmt.Println("Driver Name: " + driverName)
	fmt.Println("Host: " + host)
	fmt.Println("Port: " + strconv.Itoa(port))
	fmt.Println("User: " + user)
	fmt.Println("Password: " + password)
	fmt.Println("DB Name: " + databaseName)

	// example db.OpenDBConnection("postgres", "127.0.0.1", 5432, "postgres", "steinerj", "postgres")
	dbConnection, err := db.OpenDBConnection(driverName, host, port, user, password, databaseName)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Successfully connected to database " + dbConnection.Host + "/" + dbConnection.DatabaseName)

	result, err := dbConnection.Query("select * from golangtest")

	if err != nil {
		fmt.Println(err)
	}

	resultAsJSON, err := result.ConvertToJSON()

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(resultAsJSON)

	dbConnection.Database.Close()
}
