package main

import (
	"database/sql"
	"flag"
	"log"
	"net/http"
	"time"
)

const (
	CONNHOST = "localhost"
	CONNPORT = "7999"
	CONNTYPE = "tcp"
	HTTPHOST = "localhost"
	HTTPPORT = "8080"
)

type StoredData struct {
	UnixMillis int64  `json:"id"`
	UserId     int    `json:"user_id"`
	Data       string `json:"data"`
}

func main() {

	dbPath := flag.String("db", "file:data.db", "libsql database file path")
	ip := flag.String("ip", "", "IP addresses of other instances")
	flag.Parse()

	println("dbPath:", *dbPath)
	println("ip:", *ip)

	var DB *sql.DB
	err, DB := InitDB(*dbPath)

	if err != nil {
		log.Fatalf("InitDB error: %v", err)
	}

	// Initialize the HTTP server
	initialize_http_server(DB)

	log.Printf("Server starting on %s:%s", HTTPHOST, HTTPPORT)
	log.Fatal(http.ListenAndServe(HTTPHOST+":"+HTTPPORT, nil))

}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / 1e6
}
