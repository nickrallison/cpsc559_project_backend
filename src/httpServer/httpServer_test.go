package httpServer

import (
	"bytes"
	"cpsc559/src/database"
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

func TestPostObject(t *testing.T) {
	ts, _ := setupTestHttpServer(t)

	postObject := database.StoredObject{
		UserId:        1,
		UserMessageID: 1,
		Data:          "Hello from POST",
	}
	postBytes, err := json.Marshal(postObject)
	if err != nil {
		t.Fatalf("failed to marshal post object: %v", err)
	}

	resp, err := http.Post(ts.URL+"/objects", "application/json", bytes.NewBuffer(postBytes))
	if err != nil {
		t.Fatalf("POST /objects request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK; got %s", resp.Status)
	}

	resp, err = http.Get(ts.URL + "/objects")
	if err != nil {
		t.Fatalf("GET /objects request failed: %v", err)
	}
	var objects []database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&objects); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response: %v", err)
	}
	resp.Body.Close()

	if len(objects) != 1 {
		t.Fatalf("expected 1 object after POST; got %d", len(objects))
	}
	if objects[0].Data != postObject.Data {
		t.Errorf("expected object data %q; got %q", postObject.Data, objects[0].Data)
	}
}

func TestGetObjects(t *testing.T) {
	ts, DB := setupTestHttpServer(t)

	objectText := "Hello from DB insert"
	userId := 1
	userMessageId := 1
	if _, err := database.InsertObject(DB, userId, userMessageId, objectText); err != nil {
		t.Fatalf("InsertObject failed: %v", err)
	}

	resp, err := http.Get(ts.URL + "/objects")
	if err != nil {
		t.Fatalf("GET /objects request failed: %v", err)
	}
	var objects []database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&objects); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response: %v", err)
	}
	resp.Body.Close()

	if len(objects) != 1 {
		t.Fatalf("expected 1 object; got %d", len(objects))
	}
	if objects[0].Data != objectText {
		t.Errorf("expected object data %q; got %q", objectText, objects[0].Data)
	}
}

func TestPutObject(t *testing.T) {
	ts, DB := setupTestHttpServer(t)

	originalText := "Original object"
	userId := 1
	userMessageId := 1
	if _, err := database.InsertObject(DB, userId, userMessageId, originalText); err != nil {
		t.Fatalf("InsertObject failed: %v", err)
	}

	updateObject := database.StoredObject{
		UserId:        userId,
		UserMessageID: userMessageId,
		Data:          "Updated via PUT",
	}
	updateBytes, err := json.Marshal(updateObject)
	if err != nil {
		t.Fatalf("failed to marshal update object: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/objects", bytes.NewBuffer(updateBytes))
	if err != nil {
		t.Fatalf("failed to create PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT /objects request failed: %v", err)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/objects")
	if err != nil {
		t.Fatalf("GET /objects request failed after PUT: %v", err)
	}
	var objects []database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&objects); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response after PUT: %v", err)
	}
	resp.Body.Close()
	if len(objects) != 1 {
		t.Fatalf("expected 1 object after PUT; got %d", len(objects))
	}
	if objects[0].Data != updateObject.Data {
		t.Errorf("expected updated object data %q; got %q", updateObject.Data, objects[0].Data)
	}
}

func TestDeleteObject(t *testing.T) {
	ts, DB := setupTestHttpServer(t)

	objectText := "Object to delete"
	userId := 1
	userMessageId := 1
	if _, err := database.InsertObject(DB, userId, userMessageId, objectText); err != nil {
		t.Fatalf("InsertObject failed: %v", err)
	}

	resp, err := http.Get(ts.URL + "/objects")
	if err != nil {
		t.Fatalf("GET /objects request failed: %v", err)
	}
	var objects []database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&objects); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response: %v", err)
	}
	resp.Body.Close()

	if len(objects) != 1 {
		t.Fatalf("expected 1 object before DELETE; got %d", len(objects))
	}

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/objects?userId="+strconv.Itoa(1)+"&userMessageId="+strconv.Itoa(userId), nil)
	if err != nil {
		t.Fatalf("failed to create DELETE request: %v", err)
	}
	client := &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("DELETE /objects request failed: %v", err)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/objects")
	if err != nil {
		t.Fatalf("GET /objects request failed after DELETE: %v", err)
	}
	objects = nil
	if err := json.NewDecoder(resp.Body).Decode(&objects); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response after DELETE: %v", err)
	}
	resp.Body.Close()

	if len(objects) != 0 {
		t.Fatalf("expected 0 objects after DELETE; got %d", len(objects))
	}
}
