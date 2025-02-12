package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

func InitDB(dbPath string) (error, *sql.DB) {
	DB, err := sql.Open("libsql", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open db %s: %v", dbPath, err), nil
	}
	ctx := context.Background()
	if err = DB.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping db: %v", err), nil
	}
	createStmt := `
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            data TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    `
	if _, err = DB.ExecContext(ctx, createStmt); err != nil {
		return fmt.Errorf("failed to create messages table: %v", err), nil
	}
	return nil, DB
}

func ClearDB(dbPath string) error {
	db, err := sql.Open("libsql", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open db %s: %v", dbPath, err)
	}
	defer db.Close()

	ctx := context.Background()
	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping db: %v", err)
	}
	if _, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS messages"); err != nil {
		return fmt.Errorf("failed to drop messages table: %v", err)
	}
	return nil
}

func InsertMessage(DB *sql.DB, data string, userId int) (int, error) {
	ctx := context.Background()
	_, err := DB.ExecContext(ctx,
		"INSERT INTO messages (user_id, data) VALUES (?, ?)",
		userId, data)
	if err != nil {
		return 0, fmt.Errorf("failed to insert message: %v", err)
	}
	return 1, nil
}

func GetMessages(DB *sql.DB, userId int) ([]Message, error) {
	ctx := context.Background()
	rows, err := DB.QueryContext(ctx, "SELECT id, data FROM messages WHERE user_id = ?", userId)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %v", err)
	}
	defer rows.Close()

	msgs := []Message{}
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.Data); err != nil {
			return nil, fmt.Errorf("failed to scan message: %v", err)
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
