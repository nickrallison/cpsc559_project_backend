package database

import (
	"path/filepath"
	"testing"
)

func TestInitAndClearDB(t *testing.T) {
	testName := "TestInitAndClearDB"
	tempDir := t.TempDir()
	dbPath := "file:" + filepath.Join(tempDir, testName+".db")

	err, DB := InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB error: %v", err)
	}

	objectText := "Hello, testing!"
	userId := 1
	userMessageId := 1
	if _, err := InsertObject(DB, userId, userMessageId, objectText); err != nil {
		t.Fatalf("InsertObject failed: %v", err)
	}

	objects, err := GetObjects(DB, userId)
	if err != nil {
		t.Fatalf("GetObjects failed: %v", err)
	}
	if len(objects) != 1 {
		t.Fatalf("expected 1 object; got %d", len(objects))
	}
	if objects[0].Data != objectText {
		t.Errorf("expected object data %q; got %q", objectText, objects[0].Data)
	}

	if err := DB.Close(); err != nil {
		t.Fatalf("failed to close DB before clearing: %v", err)
	}

	if err := ClearDB(dbPath); err != nil {
		t.Fatalf("ClearDB error: %v", err)
	}

	err, DB = InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB error on reinit: %v", err)
	}

	objects, err = GetObjects(DB, userId)
	if err != nil {
		t.Fatalf("GetObjects failed after ClearDB: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("expected 0 objects after ClearDB; got %d", len(objects))
	}
	if err := DB.Close(); err != nil {
		t.Fatalf("failed to close the database: %v", err)
	}
}

func TestMultipleObjects(t *testing.T) {
	testName := "TestMultipleObjects"
	tempDir := t.TempDir()
	dbPath := "file:" + filepath.Join(tempDir, testName+".db")

	err, DB := InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB error: %v", err)
	}

	userId1 := 1
	userMessageId1 := 1
	userId2 := 2
	userMessageId2 := 1
	objectsUser1 := []string{"First object", "Second object", "Third object"}
	objectsUser2 := []string{"User2 only object"}

	for _, msg := range objectsUser1 {
		if _, err := InsertObject(DB, userId1, userMessageId1, msg); err != nil {
			t.Fatalf("failed to insert object for user 1: %v", err)
		}
	}
	for _, msg := range objectsUser2 {
		if _, err := InsertObject(DB, userId2, userMessageId2, msg); err != nil {
			t.Fatalf("failed to insert object for user 2: %v", err)
		}
	}

	msgs, err := GetObjects(DB, userId1)
	if err != nil {
		t.Fatalf("GetObjects for user 1 failed: %v", err)
	}
	if len(msgs) != len(objectsUser1) {
		t.Errorf("expected %d objects for user 1; got %d", len(objectsUser1), len(msgs))
	}
	msgs, err = GetObjects(DB, userId2)
	if err != nil {
		t.Fatalf("GetObjects for user 2 failed: %v", err)
	}
	if len(msgs) != len(objectsUser2) {
		t.Errorf("expected %d object for user 2; got %d", len(objectsUser2), len(msgs))
	}
	if err := DB.Close(); err != nil {
		t.Fatalf("failed to close the database: %v", err)
	}
}
