package database

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

type StoredObject struct {
	ID         int    `json:"id"` // primary key
	UnixMillis int64  `json:"unix_millis"`
	UserId     int    `json:"user_id"`
	Data       string `json:"data"`
}

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
        CREATE TABLE IF NOT EXISTS objects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            unixmillis INTEGER DEFAULT (strftime('%s', 'now')),
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

func InsertObject(DB *sql.DB, data string, userId int, unixmillis int64) (int, error) {
	ctx := context.Background()
	_, err := DB.ExecContext(ctx,
		"INSERT INTO objects (user_id, data, unixmillis) VALUES (?, ?, ?)",
		userId, data, unixmillis)
	if err != nil {
		return 0, fmt.Errorf("failed to insert object: %v", err)
	}
	return 1, nil
}

func GetObjects(DB *sql.DB, userId int) ([]StoredObject, error) {
	ctx := context.Background()
	rows, err := DB.QueryContext(ctx, "SELECT user_id, data, unixmillis FROM objects WHERE user_id = ?", userId)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %v", err)
	}
	defer rows.Close()

	msgs := []StoredObject{}
	for rows.Next() {
		var msg StoredObject
		if err := rows.Scan(&msg.UserId, &msg.Data, &msg.UnixMillis); err != nil {
			return nil, fmt.Errorf("failed to scan object: %v", err)
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func DeleteObject(DB *sql.DB, id int, userId int) (int, error) {
	ctx := context.Background()
	_, err := DB.ExecContext(ctx, "DELETE FROM objects WHERE id = ? AND user_id = ?", id, userId)
	if err != nil {
		return 0, fmt.Errorf("failed to delete object: %v", err)
	}
	return 1, nil
}

func UpdateObjects(DB *sql.DB, data string, id int, userId int, unixmillis int64) (int, error) {
	ctx := context.Background()
	_, err := DB.ExecContext(ctx,
		"UPDATE objects SET data = ?, unixmillis = ? WHERE id = ? AND user_id = ?",
		data, unixmillis, id, userId)
	if err != nil {
		return 0, fmt.Errorf("failed to update object: %v", err)
	}
	return 1, nil
}
