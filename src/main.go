package main

import (
    "cpsc559/src/database"
    "cpsc559/src/httpServer"
    "cpsc559/src/peer"
    "flag"
    "log"
    "net/http"
)

// main does not load .env files anymore. All config is via command-line flags.
func main() {
    // Define your command-line flags.
    dbPath := flag.String("db", "file:data.db", "libsql database file path")
    ip := flag.String("ip", "", "IP addresses of other instances")

    // For the role, fallback default can be "follower" or "leader", up to you.
    roleFlag := flag.String("role", "follower", "Role: leader or follower")

    // peerPort for peer-to-peer messaging.
    peerPort := flag.String("peerPort", "9001", "Port for peer communication")

    // If running as a follower, define the leader address to forward writes (e.g. 'localhost:9000').
    leaderAddr := flag.String("leaderAddr", "", "Leader address for follower mode")

    // For leader mode: a comma‑separated list of peer addresses (e.g. 'localhost:9001,localhost:9002').
    peersStr := flag.String("peers", "", "Comma-separated list of follower peer addresses (for leader)")

    httpHost := flag.String("httpHost", "localhost", "HTTP server host")
    httpPort := flag.String("httpPort", "8080", "HTTP server port")

    flag.Parse()

    // Convert string role to the peer.Role type.
    role := peer.RoleFromString(roleFlag)

    // Print out the config for debugging.
    log.Printf("dbPath: %s", *dbPath)
    log.Printf("ip: %s", *ip)
    log.Printf("role: %v", role)
    log.Printf("peerPort: %s", *peerPort)
    log.Printf("leaderAddr: %s", *leaderAddr)
    log.Printf("peers: %s", *peersStr)
    log.Printf("httpHost: %s", *httpHost)
    log.Printf("httpPort: %s", *httpPort)

    // Initialize the database
    err, DB := database.InitDB(*dbPath)
    if err != nil {
        log.Fatalf("InitDB error: %v", err)
    }
    defer DB.Close()

    // Create and start the peer server
    ps := peer.NewPeerServer(role, *peerPort, *leaderAddr, *peersStr, DB)
    ps.Start()
    defer ps.Stop()

    // Create the HTTP server mux using our custom handlers
    mux := httpServer.NewHTTPHandler(&ps)

    // Start serving HTTP
    log.Printf("Server starting on %s:%s", *httpHost, *httpPort)
    log.Fatal(http.ListenAndServe(*httpHost+":"+*httpPort, mux))
}
