package database

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

type StoredObject struct {
	UserId        int    `json:"user_id"`
	UserMessageID int    `json:"user_message_id"`
	Data          string `json:"data"`
}

func InitDB(dbPath string) (error, *sql.DB) {
	DB, err := sql.Open("libsql", dbPath+"?_busy_timeout=5000")
	DB.SetMaxOpenConns(1)
	if err != nil {
		return fmt.Errorf("failed to open db %s: %v", dbPath, err), nil
	}
	ctx := context.Background()
	if err = DB.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping db: %v", err), nil
	}
	createStmt := `
        CREATE TABLE IF NOT EXISTS objects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            user_message_id INTEGER,
            data TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    `
	if _, err = DB.ExecContext(ctx, createStmt); err != nil {
		return fmt.Errorf("failed to create objects table: %v", err), nil
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
	if _, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS objects"); err != nil {
		return fmt.Errorf("failed to drop objects table: %v", err)
	}
	return nil
}

func InsertObject(DB *sql.DB, userId int, user_message_id int, data string) (int, error) {
	ctx := context.Background()
	_, err := DB.ExecContext(ctx,
		"INSERT INTO objects (user_id, user_message_id, data) VALUES (?, ?, ?)",
		userId, user_message_id, data)
	if err != nil {
		return 0, fmt.Errorf("failed to insert object: %v", err)
	}
	return 1, nil
}

func GetObjects(DB *sql.DB, userId int) ([]StoredObject, error) {
	ctx := context.Background()
	rows, err := DB.QueryContext(ctx, "SELECT user_id, user_message_id, data FROM objects WHERE user_id = ?", userId)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %v", err)
	}
	defer rows.Close()

	msgs := []StoredObject{}
	for rows.Next() {
		var msg StoredObject
		if err := rows.Scan(&msg.UserId, &msg.UserMessageID, &msg.Data); err != nil {
			return nil, fmt.Errorf("failed to scan object: %v", err)
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func DeleteObject(DB *sql.DB, userId int, user_message_id int) (int, error) {
	ctx := context.Background()
	_, err := DB.ExecContext(ctx, "DELETE FROM objects WHERE user_id = ? AND user_message_id = ?", userId, user_message_id)
	if err != nil {
		return 0, fmt.Errorf("failed to delete object: %v", err)
	}
	return 1, nil
}

func UpdateObjects(DB *sql.DB, userId int, user_message_id int, data string) (int, error) {
	ctx := context.Background()
	_, err := DB.ExecContext(ctx, "UPDATE objects SET data = ? WHERE user_id = ? AND user_message_id = ?", data, userId, user_message_id)
	if err != nil {
		return 0, fmt.Errorf("failed to update object: %v", err)
	}
	return 1, nil
}
