package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"strconv"
	"time"
)

type Message struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

func main() {

	dbPath := flag.String("db", "file:messages.db", "libsql database file path")
	flag.Parse()

	var DB *sql.DB
	err, DB := InitDB(*dbPath)

	if err != nil {
		log.Fatalf("InitDB error: %v", err)
	}

	var unixTime int64
	unixTime = time.Now().Unix()

	messageText := "Hello, testing!"
	messageJson := `{"time":` + strconv.FormatInt(unixTime, 10) + `,"message":"` + messageText + `"}`

	userId := 1
	if _, err := InsertMessage(DB, messageJson, userId); err != nil {
		log.Fatalf("InsertMessage failed: %v", err)
	}

	// Get messages

	messages, err := GetMessages(DB, userId)
	if err != nil {
		log.Fatalf("GetMessages failed: %v", err)
	}
	println("Messages:")
	for _, m := range messages {

		var data map[string]interface{}
		var time int64
		var message string
		if err := json.Unmarshal([]byte(m.Data), &data); err != nil {
			log.Fatalf("json.Unmarshal failed: %v", err)
		}
		time = int64(data["time"].(float64))
		message = data["message"].(string)
		println("ID:", m.ID, "Time:", time, "Message:", message)
	}

}
