package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/urimeba/databricks-sql-go"
	"github.com/urimeba/databricks-sql-go/driverctx"
	dbsqllog "github.com/urimeba/databricks-sql-go/logger"
)

func main() {
	// Opening a driver typically will not attempt to connect to the database.
	if err := dbsqllog.SetLogLevel("debug"); err != nil {
		panic(err)
	}
	err := godotenv.Load()

	if err != nil {
		log.Fatal(err.Error())
	}
	db, err := sql.Open("databricks", fmt.Sprintf("token:%s@%s:%s%s", os.Getenv("DATABRICKS_ACCESSTOKEN"), os.Getenv("DATABRICKS_HOST"), os.Getenv("DATABRICKS_PORT"), os.Getenv("DATABRICKS_HTTPPATH")))

	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}
	defer db.Close()

	ogCtx := driverctx.NewContextWithCorrelationId(context.Background(), "context-timeout-example")
	ctx1, cancel1 := context.WithTimeout(ogCtx, 10*time.Second)
	defer cancel1()
	var res int
	if err := db.QueryRowContext(ctx1, `SELECT id FROM RANGE(100000000) ORDER BY RANDOM() + 2 asc`).Scan(&res); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res)
	}

}
