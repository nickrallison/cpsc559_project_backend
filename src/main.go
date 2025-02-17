package main

import (
	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"database/sql"
	"flag"
	"log"
	"net/http"
)

const (
	CONNHOST = "localhost"
	CONNPORT = "7999"
	CONNTYPE = "tcp"
	HTTPHOST = "localhost"
	HTTPPORT = "8080"
)

func main() {

	dbPath := flag.String("db", "file:data.db", "libsql database file path")
	ip := flag.String("ip", "", "IP addresses of other instances")
	flag.Parse()

	println("dbPath:", *dbPath)
	println("ip:", *ip)

	var DB *sql.DB
	err, DB := database.InitDB(*dbPath)

	if err != nil {
		log.Fatalf("InitDB error: %v", err)
	}

	httpServer.Initialize_http_server(DB)

	log.Printf("Server starting on %s:%s", HTTPHOST, HTTPPORT)
	log.Fatal(http.ListenAndServe(HTTPHOST+":"+HTTPPORT, nil))

}
