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
		AllowedOrigins:   []string{"http://localhost:3000"},
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

// postObjectHandler handles POST requests.
func postObjectHandler(w http.ResponseWriter, r *http.Request, ps *peer.PeerServer) {
	var objects []database.StoredObject
	if err := json.NewDecoder(r.Body).Decode(&objects); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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

// putObjectHandler handles PUT requests by decoding a single object and calling UpdateObject.
func putObjectHandler(w http.ResponseWriter, r *http.Request, ps *peer.PeerServer) {
	var obj database.StoredObject
	if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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

// deleteObjectHandler handles DELETE requests by reading “userId” and “userMessageId” parameters
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
