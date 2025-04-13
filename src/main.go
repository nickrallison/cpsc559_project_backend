package main

import (
	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"cpsc559/src/peer"
	"database/sql"
	"flag"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
)

func main() {

	if err := godotenv.Load(os.Getenv("ENV_FILE")); err != nil {
		log.Printf("Cannot load .env file: %v", err)
		log.Printf("Loading default .env file: \".env.leader\"")
		godotenv.Load(".env.leader")
	}

	// Parse command-line flags.
	dbPath := flag.String("db", os.Getenv("DATABASE"), "libsql database file path")
	ip := flag.String("ip", "", "IP addresses of other instances")
	role := peer.RoleFromString(flag.String("role", os.Getenv("ROLE"), "Role: leader or follower"))

	// peerPort for peer-to-peer messaging.
	localAddr := flag.String("localAddr", os.Getenv("MYOWN"), "Local address for peer communication")
	peerPort := flag.String("peerPort", os.Getenv("PEERPORT"), "Port for peer communication")
	// When running as a follower, leaderAddr defines where to forward writes.
	leaderAddr := flag.String("leaderAddr", os.Getenv("LEADERADDR"), "Leader address for follower mode (e.g. 'localhost:9000')")
	// For leader mode: comma‑separated list of peer addresses.
	peersStr := flag.String("peers", os.Getenv("PEERS"), "Comma-separated list of follower peer addresses (for leader)")
	httpHost := flag.String("httpHost", os.Getenv("MYOWN"), "HTTP server host")
	httpPort := flag.String("httpPort", os.Getenv("HTTPPORT"), "HTTP server port")

	// If localAddr ends in ":", strip it.
	if len(*localAddr) > 0 && (*localAddr)[len(*localAddr)-1] == ':' {
		*localAddr = (*localAddr)[:len(*localAddr)-1]
	}

	flag.Parse()

	println("dbPath:", *dbPath)
	println("ip:", *ip)
	println("role:", role)
	println("peerPort:", *peerPort)
	println("leaderAddr:", *leaderAddr)
	println("peers:", *peersStr)

	var err error
	var DB *sql.DB
	err = database.ClearDB(*dbPath)
	if err != nil {
		log.Printf("ClearDB error: %v", err)
	}
	err, DB = database.InitDB(*dbPath)
	if err != nil {
		log.Fatalf("InitDB error: %v", err)
	}

	simulateDelay := false
	ps := peer.NewPeerServer(role, *peerPort, *localAddr, *leaderAddr, *peersStr, DB, simulateDelay, 0.0, 0.0)
	ps.Start()
	defer ps.Stop()

	mux := httpServer.NewHTTPHandler(&ps)

	log.Printf("Server starting on %s:%s", *httpHost, *httpPort)
	log.Fatal(http.ListenAndServe(*httpHost+":"+*httpPort, mux))
}
