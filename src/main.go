package main

import (
	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"cpsc559/src/peer"
	"database/sql"
	"flag"
	"log"
	"net/http"
	"os"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(os.Getenv("ENV_FILE")); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	
	// Parse command-line flags.
	dbPath := flag.String("db", "file:data.db", "libsql database file path")
	ip := flag.String("ip", "", "IP addresses of other instances")
	role := peer.RoleFromString(flag.String("role", os.Getenv("ROLE"), "Role: leader or follower"))

	// peerPort for peer-to-peer messaging.
	peerPort := flag.String("peerPort", os.Getenv("PEERPORT"), "Port for peer communication")
	// When running as a follower, leaderAddr defines where to forward writes.
	leaderAddr := flag.String("leaderAddr", "", "Leader address for follower mode (e.g. 'localhost:9000')")
	// For leader mode: comma‑separated list of peer addresses.
	peersStr := flag.String("peers", "", "Comma-separated list of follower peer addresses (for leader)")
	httpHost := flag.String("httpHost", "localhost", "HTTP server host")
	httpPort := flag.String("httpPort", os.Getenv("HTTPPORT"), "HTTP server port")

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

	log.Printf("Server starting on %s:%s", *httpHost, *httpPort)
	log.Fatal(http.ListenAndServe(*httpHost+":"+*httpPort, mux))
}
