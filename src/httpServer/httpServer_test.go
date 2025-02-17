package httpServer

import (
	"bytes"
	"cpsc559/src/database"
	"cpsc559/src/util"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
)

func setupTestHttpServer(t *testing.T) (*httptest.Server, *sql.DB) {
	t.Helper()

	mux := http.NewServeMux()
	origMux := http.DefaultServeMux
	http.DefaultServeMux = mux

	tempDir := t.TempDir()
	dbPath := "file:" + filepath.Join(tempDir, "http_server_test.db")

	err, DB := database.InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB error: %v", err)
	}

	Initialize_http_server(DB)

	ts := httptest.NewServer(mux)

	t.Cleanup(func() {
		ts.Close()
		DB.Close()
		http.DefaultServeMux = origMux
	})

	return ts, DB
}

func TestPostMessage(t *testing.T) {
	ts, _ := setupTestHttpServer(t)

	postMessage := database.StoredData{
		UserId:     1,
		Data:       "Hello from POST",
		UnixMillis: util.MakeTimestamp(),
	}
	postBytes, err := json.Marshal(postMessage)
	if err != nil {
		t.Fatalf("failed to marshal post message: %v", err)
	}

	resp, err := http.Post(ts.URL+"/messages", "application/json", bytes.NewBuffer(postBytes))
	if err != nil {
		t.Fatalf("POST /messages request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK; got %s", resp.Status)
	}

	resp, err = http.Get(ts.URL + "/messages")
	if err != nil {
		t.Fatalf("GET /messages request failed: %v", err)
	}
	var messages []database.StoredData
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response: %v", err)
	}
	resp.Body.Close()

	if len(messages) != 1 {
		t.Fatalf("expected 1 message after POST; got %d", len(messages))
	}
	if messages[0].Data != postMessage.Data {
		t.Errorf("expected message data %q; got %q", postMessage.Data, messages[0].Data)
	}
}

func TestGetMessages(t *testing.T) {
	ts, DB := setupTestHttpServer(t)

	unixMillis := util.MakeTimestamp()
	messageText := "Hello from DB insert"
	userId := 1
	if _, err := database.InsertMessage(DB, messageText, userId, unixMillis); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	resp, err := http.Get(ts.URL + "/messages")
	if err != nil {
		t.Fatalf("GET /messages request failed: %v", err)
	}
	var messages []database.StoredData
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response: %v", err)
	}
	resp.Body.Close()

	if len(messages) != 1 {
		t.Fatalf("expected 1 message; got %d", len(messages))
	}
	if messages[0].Data != messageText {
		t.Errorf("expected message data %q; got %q", messageText, messages[0].Data)
	}
}

func TestPutMessage(t *testing.T) {
	ts, DB := setupTestHttpServer(t)

	unixMillis := util.MakeTimestamp()
	originalText := "Original message"
	userId := 1
	if _, err := database.InsertMessage(DB, originalText, userId, unixMillis); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	updateMessage := database.StoredData{
		ID:         1,
		UserId:     userId,
		Data:       "Updated via PUT",
		UnixMillis: util.MakeTimestamp(),
	}
	updateBytes, err := json.Marshal(updateMessage)
	if err != nil {
		t.Fatalf("failed to marshal update message: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/messages", bytes.NewBuffer(updateBytes))
	if err != nil {
		t.Fatalf("failed to create PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT /messages request failed: %v", err)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/messages")
	if err != nil {
		t.Fatalf("GET /messages request failed after PUT: %v", err)
	}
	var messages []database.StoredData
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response after PUT: %v", err)
	}
	resp.Body.Close()
	if len(messages) != 1 {
		t.Fatalf("expected 1 message after PUT; got %d", len(messages))
	}
	if messages[0].Data != updateMessage.Data {
		t.Errorf("expected updated message data %q; got %q", updateMessage.Data, messages[0].Data)
	}
}

func TestDeleteMessage(t *testing.T) {
	ts, DB := setupTestHttpServer(t)

	unixMillis := util.MakeTimestamp()
	messageText := "Message to delete"
	userId := 1
	if _, err := database.InsertMessage(DB, messageText, userId, unixMillis); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	resp, err := http.Get(ts.URL + "/messages")
	if err != nil {
		t.Fatalf("GET /messages request failed: %v", err)
	}
	var messages []database.StoredData
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response: %v", err)
	}
	resp.Body.Close()

	if len(messages) != 1 {
		t.Fatalf("expected 1 message before DELETE; got %d", len(messages))
	}

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/messages?id="+strconv.Itoa(1)+"&userId="+strconv.Itoa(userId), nil)
	if err != nil {
		t.Fatalf("failed to create DELETE request: %v", err)
	}
	client := &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("DELETE /messages request failed: %v", err)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/messages")
	if err != nil {
		t.Fatalf("GET /messages request failed after DELETE: %v", err)
	}
	messages = nil
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response after DELETE: %v", err)
	}
	resp.Body.Close()

	if len(messages) != 0 {
		t.Fatalf("expected 0 messages after DELETE; got %d", len(messages))
	}
}
