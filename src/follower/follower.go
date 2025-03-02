package follower

import (
	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"cpsc559/src/peer"
	"database/sql"
	"log"
	"net/http"

	"github.com/rs/cors"
)

var DB *sql.DB
var followerPeer *peer.PeerServer

// InitializeFollower sets up the follower server (only handling GET requests)
// using its own database and a dedicated PeerServer instance.
func InitializeFollower() {
	// Use a dedicated DB file for the follower
	dbPath := "file:follower_data.db"
	port := "8081"

	var err error
	err, DB = database.InitDB(dbPath)
	if err != nil {
		log.Fatalf("InitDB error: %v", err)
	}

	// Create a follower peer server instance.
	// (If your follower is supposed to forward write requests, set the LeaderAddr accordingly.)
	ps := peer.NewPeerServer(peer.Follower, port, "", "", DB)
	followerPeer = &ps

	// Set up CORS rules so that the frontend can access our endpoint.
	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000"},
		AllowedMethods:   []string{"GET"},
		AllowedHeaders:   []string{"Content-Type"},
		AllowCredentials: true,
	})

	// Create a mux that only handles GET requests.
	mux := http.NewServeMux()
	mux.Handle("/objects", corsHandler.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// Pass the follower PeerServer to GetObjectsHandler
			httpServer.GetObjectsHandler(w, r, followerPeer)
		} else {
			http.Error(w, "Only reads are allowed", http.StatusMethodNotAllowed)
		}
	})))

	// Start the HTTP server in a goroutine.
	go func() {
		log.Printf("Follower running on %s\n", port)
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			log.Fatalf("ListenAndServe error: %v", err)
		}
	}()
}

// CleanupFollower should be called when the follower is to shut down (for example, at the end of a test).
// It closes the DB connection and clears the follower database.
func CleanupFollower() {
	if DB != nil {
		if err := DB.Close(); err != nil {
			log.Printf("Error closing DB: %v", err)
		}
		if err := database.ClearDB("file:follower_data.db"); err != nil {
			log.Printf("Error clearing follower DB: %v", err)
		}
	}
}
