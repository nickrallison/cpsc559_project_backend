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

	getMessagesHandler := func(w http.ResponseWriter, r *http.Request) {

		userId := 1

		// Get messages
		messages, err := database.GetMessages(DB, userId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(messages)
	}

	postMessageHandler := func(w http.ResponseWriter, r *http.Request) {

		var message database.StoredData
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		userId := message.UserId
		messageJson := message.Data
		unixmillis := message.UnixMillis

		if _, err := database.InsertMessage(DB, messageJson, userId, unixmillis); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	deleteMessageHandler := func(w http.ResponseWriter, r *http.Request) {
		idParam := r.URL.Query().Get("id")
		id, err := strconv.Atoi(idParam)

		userIdParam := r.URL.Query().Get("userId")
		userId, err := strconv.Atoi(userIdParam)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if _, err := database.DeleteMessage(DB, id, userId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	putMessageHandler := func(w http.ResponseWriter, r *http.Request) {
		var message database.StoredData
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		id := message.ID
		userId := message.UserId
		messageJson := message.Data
		unixmillis := message.UnixMillis

		if _, err := database.UpdateMessages(DB, messageJson, id, userId, unixmillis); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	handleMessages := func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getMessagesHandler(w, r)
		case http.MethodPost:
			postMessageHandler(w, r)
		case http.MethodPut:
			putMessageHandler(w, r)
		case http.MethodDelete:
			deleteMessageHandler(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}

	var routes = []Route{
		newRoute("/messages", handleMessages),
	}

	for _, r := range routes {
		http.HandleFunc(r.path, r.handler)
	}

}
