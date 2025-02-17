package database

import (
	"cpsc559/src/util"
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

	unixMillis := util.MakeTimestamp()
	messageText := "Hello, testing!"
	userId := 1
	if _, err := InsertMessage(DB, messageText, userId, unixMillis); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	messages, err := GetMessages(DB, userId)
	if err != nil {
		t.Fatalf("GetMessages failed: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message; got %d", len(messages))
	}
	if messages[0].Data != messageText {
		t.Errorf("expected message data %q; got %q", messageText, messages[0].Data)
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

	messages, err = GetMessages(DB, userId)
	if err != nil {
		t.Fatalf("GetMessages failed after ClearDB: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages after ClearDB; got %d", len(messages))
	}
	if err := DB.Close(); err != nil {
		t.Fatalf("failed to close the database: %v", err)
	}
}

func TestMultipleMessages(t *testing.T) {
	testName := "TestMultipleMessages"
	tempDir := t.TempDir()
	dbPath := "file:" + filepath.Join(tempDir, testName+".db")

	err, DB := InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB error: %v", err)
	}

	userId1 := 1
	userId2 := 2
	messagesUser1 := []string{"First message", "Second message", "Third message"}
	messagesUser2 := []string{"User2 only message"}

	unixMillis1 := util.MakeTimestamp()
	unixMillis2 := util.MakeTimestamp()

	for _, msg := range messagesUser1 {
		if _, err := InsertMessage(DB, msg, userId1, unixMillis1); err != nil {
			t.Fatalf("failed to insert message for user 1: %v", err)
		}
	}
	for _, msg := range messagesUser2 {
		if _, err := InsertMessage(DB, msg, userId2, unixMillis2); err != nil {
			t.Fatalf("failed to insert message for user 2: %v", err)
		}
	}

	msgs, err := GetMessages(DB, userId1)
	if err != nil {
		t.Fatalf("GetMessages for user 1 failed: %v", err)
	}
	if len(msgs) != len(messagesUser1) {
		t.Errorf("expected %d messages for user 1; got %d", len(messagesUser1), len(msgs))
	}
	msgs, err = GetMessages(DB, userId2)
	if err != nil {
		t.Fatalf("GetMessages for user 2 failed: %v", err)
	}
	if len(msgs) != len(messagesUser2) {
		t.Errorf("expected %d message for user 2; got %d", len(messagesUser2), len(msgs))
	}
	if err := DB.Close(); err != nil {
		t.Fatalf("failed to close the database: %v", err)
	}
}
