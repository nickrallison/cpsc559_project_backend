package follower

import (
	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"database/sql"
	"log"
	"net/http"

	"github.com/rs/cors"
)

var DB *sql.DB

func InitializeFollower() {
	dbPath := "file:data.db"
	port := "8081"

	// Initializing follower
	var err error
	err, DB = database.InitDB(dbPath)
	if err != nil {
		log.Fatalf("InitDB error: %v", err)
	}

	// CORS rules modification so that it is able to connec to the frontend
	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000"},
		AllowedMethods:   []string{"GET"},
		AllowedHeaders:   []string{"Content-Type"},
		AllowCredentials: true,
	})

	// only handles read requests
	mux := http.NewServeMux()
	mux.Handle("/objects", corsHandler.Handler(http.HandlerFunc(handleReads)))

	log.Printf("Follower running on %s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// Handles only GET requests form clients
func handleReads(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		httpServer.GetObjectsHandler(DB, w, r)
	} else {
		http.Error(w, "Only reads are allowed", http.StatusMethodNotAllowed)
	}

}
