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

// setupTestHttpServer sets up a new mux and database for testing.
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
	InitializeHttpServer(DB)
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

	objectsToPost := []database.StoredObject{
		{
			UserId:        1,
			UserMessageID: 1,
			Data:          "Hello from POST array",
		},
	}
	postBytes, err := json.Marshal(objectsToPost)
	if err != nil {
		t.Fatalf("failed to marshal post object array: %v", err)
	}
	resp, err := http.Post(ts.URL+"/objects", "application/json", bytes.NewBuffer(postBytes))
	if err != nil {
		t.Fatalf("POST /objects request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK; got %s", resp.Status)
	}

	// Verify the object was inserted.
	resp, err = http.Get(ts.URL + "/objects?userId=1")
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
	if objects[0].Data != objectsToPost[0].Data {
		t.Errorf("expected object data %q; got %q", objectsToPost[0].Data, objects[0].Data)
	}
}

func TestPostMultipleObjects(t *testing.T) {
	ts, _ := setupTestHttpServer(t)
	// Post multiple objects at once.
	objectsToPost := []database.StoredObject{
		{
			UserId:        2,
			UserMessageID: 1,
			Data:          "First object for user2",
		},
		{
			UserId:        2,
			UserMessageID: 2,
			Data:          "Second object for user2",
		},
	}
	postBytes, err := json.Marshal(objectsToPost)
	if err != nil {
		t.Fatalf("failed to marshal post objects array: %v", err)
	}
	resp, err := http.Post(ts.URL+"/objects", "application/json", bytes.NewBuffer(postBytes))
	if err != nil {
		t.Fatalf("POST /objects request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK; got %s", resp.Status)
	}
	// Verify both objects were inserted.
	resp, err = http.Get(ts.URL + "/objects?userId=2")
	if err != nil {
		t.Fatalf("GET /objects request failed: %v", err)
	}
	var objects []database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&objects); err != nil {
		resp.Body.Close()
		t.Fatalf("failed to decode GET response: %v", err)
	}
	resp.Body.Close()
	if len(objects) != len(objectsToPost) {
		t.Fatalf("expected %d objects after POST; got %d", len(objectsToPost), len(objects))
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
	resp, err := http.Get(ts.URL + "/objects?userId=1")
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

func TestGetSingleObject(t *testing.T) {
	ts, DB := setupTestHttpServer(t)
	// Insert multiple objects for the same user.
	userId := 3
	objectsToInsert := []database.StoredObject{
		{UserId: userId, UserMessageID: 1, Data: "Message 1"},
		{UserId: userId, UserMessageID: 2, Data: "Message 2"},
	}
	for _, obj := range objectsToInsert {
		if _, err := database.InsertObject(DB, obj.UserId, obj.UserMessageID, obj.Data); err != nil {
			t.Fatalf("InsertObject failed: %v", err)
		}
	}
	// Request a specific object using userMessageId=2.
	resp, err := http.Get(ts.URL + "/objects?userId=3&userMessageId=2")
	if err != nil {
		t.Fatalf("GET /objects with userMessageId request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK; got %d", resp.StatusCode)
	}
	// In this case the handler returns a single object (not an array)
	var obj database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		t.Fatalf("failed to decode GET single object response: %v", err)
	}
	if obj.UserMessageID != 2 {
		t.Errorf("expected userMessageID %d; got %d", 2, obj.UserMessageID)
	}
	if obj.Data != "Message 2" {
		t.Errorf("expected data %q; got %q", "Message 2", obj.Data)
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
	resp, err = http.Get(ts.URL + "/objects?userId=1")
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
	resp, err := http.Get(ts.URL + "/objects?userId=1")
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
	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/objects?userId="+strconv.Itoa(userId)+"&userMessageId="+strconv.Itoa(userMessageId), nil)
	if err != nil {
		t.Fatalf("failed to create DELETE request: %v", err)
	}
	client := &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("DELETE /objects request failed: %v", err)
	}
	resp.Body.Close()
	resp, err = http.Get(ts.URL + "/objects?userId=1")
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
