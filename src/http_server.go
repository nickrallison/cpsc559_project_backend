package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
)

type route struct {
	method  string
	path    string
	handler func(w http.ResponseWriter, r *http.Request)
}

func newRoute(path string, handler func(w http.ResponseWriter, r *http.Request)) route {
	return route{
		path:    path,
		handler: handler,
	}
}

func initialize_http_server(DB *sql.DB) {

	getMessagesAnonymous := func(w http.ResponseWriter, r *http.Request) {

		userId := 1

		// Get messages
		messages, err := GetMessages(DB, userId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(messages)
	}

	postMessageAnonymous := func(w http.ResponseWriter, r *http.Request) {

		var message StoredData
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		userId := message.UserId
		messageJson := message.Data
		unixmillis := message.UnixMillis

		if _, err := InsertMessage(DB, messageJson, userId, unixmillis); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	handleMessages := func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			postMessageAnonymous(w, r)
		case http.MethodGet:
			getMessagesAnonymous(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}

	var routes = []route{
		newRoute("/messages", handleMessages),
	}

	for _, r := range routes {
		http.HandleFunc(r.path, r.handler)
	}

}
