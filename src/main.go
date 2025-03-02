package main

import (
	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"cpsc559/src/peer"
	"database/sql"
	"flag"
	"log"
	"net/http"
)

const (
	HTTPHOST = "localhost"
	HTTPPORT = "8080"
)

func main() {
	// Parse command-line flags.
	dbPath := flag.String("db", "file:data.db", "libsql database file path")
	ip := flag.String("ip", "", "IP addresses of other instances")
	role := peer.RoleFromString(flag.String("role", "leader", "Role: leader or follower"))

	// peerPort for peer-to-peer messaging.
	peerPort := flag.String("peerPort", "9000", "Port for peer communication")
	// When running as a follower, leaderAddr defines where to forward writes.
	leaderAddr := flag.String("leaderAddr", "", "Leader address for follower mode (e.g. 'localhost:9000')")
	// For leader mode: comma‑separated list of peer addresses.
	peersStr := flag.String("peers", "", "Comma-separated list of follower peer addresses (for leader)")

	flag.Parse()

	println("dbPath:", *dbPath)
	println("ip:", *ip)
	println("role:", role)
	println("peerPort:", *peerPort)
	println("leaderAddr:", *leaderAddr)
	println("peers:", *peersStr)

	var err error
	var DB *sql.DB
	err, DB = database.InitDB(*dbPath)
	if err != nil {
		log.Fatalf("InitDB error: %v", err)
	}

	ps := peer.NewPeerServer(role, *peerPort, *leaderAddr, *peersStr, DB)
	ps.Start()
	defer ps.Stop()

	mux := httpServer.NewHTTPHandler(&ps)

	log.Printf("Server starting on %s:%s", HTTPHOST, HTTPPORT)
	log.Fatal(http.ListenAndServe(HTTPHOST+":"+HTTPPORT, mux))
}
