package main

import (
	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"cpsc559/src/follower"
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

	httpServer.InitializeHttpServer(DB)

	
	// starting follower as a separte go routine using the keyowrd: go
	go follower.InitializeFollower()
	log.Printf("Server starting on %s:%s", HTTPHOST, HTTPPORT)
	log.Fatal(http.ListenAndServe(HTTPHOST+":"+HTTPPORT, nil))
}
