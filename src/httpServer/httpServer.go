package httpServer

import (
	"cpsc559/src/database"
	"cpsc559/src/peer"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/rs/cors"
)

func NewHTTPHandler(ps *peer.PeerServer) http.Handler {
	mux := http.NewServeMux()

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:3001", "http://localhost:3002"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	})

	mux.HandleFunc("/objects", corsHandler.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			GetObjectsHandler(w, r, ps)

		case http.MethodPost:
			postObjectHandler(w, r, ps)

		case http.MethodPut:
			putObjectHandler(w, r, ps)

		case http.MethodDelete:
			deleteObjectHandler(w, r, ps)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})).ServeHTTP)

	mux.HandleFunc("/allObjects", corsHandler.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			GetAllObjectsHandler(w, ps)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})).ServeHTTP)

	return mux
}

// GetObjectsHandler handles GET requests.
func GetObjectsHandler(w http.ResponseWriter, r *http.Request, ps *peer.PeerServer) {
	userIdParam := r.URL.Query().Get("userId")
	if userIdParam == "" {
		http.Error(w, "missing userId parameter", http.StatusBadRequest)
		return
	}
	userId, err := strconv.Atoi(userIdParam)
	if err != nil {
		http.Error(w, "invalid userId parameter", http.StatusBadRequest)
		return
	}
	userMessageIdParam := r.URL.Query().Get("userMessageId")
	w.Header().Set("Content-Type", "application/json")
	if userMessageIdParam != "" {
		userMessageId, err := strconv.Atoi(userMessageIdParam)
		if err != nil {
			http.Error(w, "invalid userMessageId parameter", http.StatusBadRequest)
			return
		}
		obj, err := ps.GetObject(userId, userMessageId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := json.NewEncoder(w).Encode(obj); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		objs, err := ps.GetObjects(userId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := json.NewEncoder(w).Encode(objs); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// GetAllObjectsHandler retrieves all objects from the database
func GetAllObjectsHandler(w http.ResponseWriter, ps *peer.PeerServer) {
	objs, err := ps.GetAllObjects()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(objs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// postObjectHandler handles POST requests with sequence numbers.
func postObjectHandler(w http.ResponseWriter, r *http.Request, ps *peer.PeerServer) {
	var objects []database.StoredObject
	if err := json.NewDecoder(r.Body).Decode(&objects); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Automatically assign sequence numbers if this node is a leader
	for i := range objects {
		if ps.Role == peer.Leader {
			ps.OpMutex.Lock()
			objects[i].SequenceNumber = ps.NextSequenceID
			ps.NextSequenceID++
			ps.OpMutex.Unlock()
		}
	}

	resp, err := ps.StoreObjects(objects)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// putObjectHandler handles PUT requests with sequence numbers.
func putObjectHandler(w http.ResponseWriter, r *http.Request, ps *peer.PeerServer) {
	var obj database.StoredObject
	if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if ps.Role == peer.Leader {
		ps.OpMutex.Lock()
		obj.SequenceNumber = ps.NextSequenceID
		ps.NextSequenceID++
		ps.OpMutex.Unlock()
	}

	resp, err := ps.UpdateObject(obj)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// deleteObjectHandler handles DELETE requests while ensuring sequence number consistency.
func deleteObjectHandler(w http.ResponseWriter, r *http.Request, ps *peer.PeerServer) {
	userIdParam := r.URL.Query().Get("userId")
	userId, err := strconv.Atoi(userIdParam)
	if err != nil {
		http.Error(w, "invalid userId parameter", http.StatusBadRequest)
		return
	}
	userMessageIdParam := r.URL.Query().Get("userMessageId")
	userMessageId, err := strconv.Atoi(userMessageIdParam)
	if err != nil {
		http.Error(w, "invalid userMessageId parameter", http.StatusBadRequest)
		return
	}

	resp, err := ps.DeleteObject(userId, userMessageId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
