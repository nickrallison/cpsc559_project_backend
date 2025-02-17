package httpServer

import (
	"cpsc559/src/database"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

type Route struct {
	path    string
	handler func(w http.ResponseWriter, r *http.Request)
}

func newRoute(path string, handler func(w http.ResponseWriter, r *http.Request)) Route {
	return Route{
		path:    path,
		handler: handler,
	}
}

func Initialize_http_server(DB *sql.DB) {

	getObjectsHandler := func(w http.ResponseWriter, r *http.Request) {
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
		if userMessageIdParam != "" {
			userMessageId, err := strconv.Atoi(userMessageIdParam)
			if err != nil {
				http.Error(w, "invalid userMessageId parameter", http.StatusBadRequest)
				return
			}
			ctx := r.Context()
			query := "SELECT user_id, user_message_id, data FROM objects WHERE user_id = ? AND user_message_id = ?"
			row := DB.QueryRowContext(ctx, query, userId, userMessageId)
			var obj database.StoredObject
			if err := row.Scan(&obj.UserId, &obj.UserMessageID, &obj.Data); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					http.Error(w, "object not found", http.StatusNotFound)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(obj); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			return
		}

		objects, err := database.GetObjects(DB, userId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(objects); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	postObjectHandler := func(w http.ResponseWriter, r *http.Request) {

		var objects []database.StoredObject
		err := json.NewDecoder(r.Body).Decode(&objects)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, object := range objects {
			userId := object.UserId
			userMessageId := object.UserMessageID
			objectJson := object.Data
			if _, err := database.InsertObject(DB, userId, userMessageId, objectJson); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}

	deleteObjectHandler := func(w http.ResponseWriter, r *http.Request) {

		userIdParam := r.URL.Query().Get("userId")
		userId, err := strconv.Atoi(userIdParam)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		userMessageIdParam := r.URL.Query().Get("userMessageId")
		userMessageId, err := strconv.Atoi(userMessageIdParam)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if _, err := database.DeleteObject(DB, userId, userMessageId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	putObjectHandler := func(w http.ResponseWriter, r *http.Request) {
		var object database.StoredObject
		err := json.NewDecoder(r.Body).Decode(&object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		userId := object.UserId
		userMessageId := object.UserMessageID
		objectJson := object.Data

		if _, err := database.UpdateObjects(DB, userId, userMessageId, objectJson); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	handleObjects := func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getObjectsHandler(w, r)
		case http.MethodPost:
			postObjectHandler(w, r)
		case http.MethodPut:
			putObjectHandler(w, r)
		case http.MethodDelete:
			deleteObjectHandler(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}

	var routes = []Route{
		newRoute("/objects", handleObjects),
	}

	for _, r := range routes {
		http.HandleFunc(r.path, r.handler)
	}

}
