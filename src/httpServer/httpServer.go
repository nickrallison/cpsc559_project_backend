package httpServer

import (
	"cpsc559/src/database"
	"database/sql"
	"encoding/json"
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

		userId := 1

		// Get objects
		objects, err := database.GetObjects(DB, userId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(objects)
	}

	postObjectHandler := func(w http.ResponseWriter, r *http.Request) {

		var object database.StoredData
		err := json.NewDecoder(r.Body).Decode(&object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		userId := object.UserId
		objectJson := object.Data
		unixmillis := object.UnixMillis

		if _, err := database.InsertObject(DB, objectJson, userId, unixmillis); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	deleteObjectHandler := func(w http.ResponseWriter, r *http.Request) {
		idParam := r.URL.Query().Get("id")
		id, err := strconv.Atoi(idParam)

		userIdParam := r.URL.Query().Get("userId")
		userId, err := strconv.Atoi(userIdParam)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if _, err := database.DeleteObject(DB, id, userId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	putObjectHandler := func(w http.ResponseWriter, r *http.Request) {
		var object database.StoredData
		err := json.NewDecoder(r.Body).Decode(&object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		id := object.ID
		userId := object.UserId
		objectJson := object.Data
		unixmillis := object.UnixMillis

		if _, err := database.UpdateObjects(DB, objectJson, id, userId, unixmillis); err != nil {
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
