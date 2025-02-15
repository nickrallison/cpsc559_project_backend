package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"strconv"
	"time"
)

const (
	CONNHOST = "localhost"
	CONNPORT = "7999"
	CONNTYPE = "tcp"
)

type StoredData struct {
	UnixMillis int64  `json:"id"`
	UserId     int    `json:"user_id"`
	Data       string `json:"data"`
}

func main() {

	dbPath := flag.String("db", "file:data.db", "libsql database file path")
	ip := flag.String("ip", "", "IP addresses of other instances")
	flag.Parse()

	println("dbPath:", *dbPath)
	println("ip:", *ip)

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
	var unixmillis = makeTimestamp()
	if _, err := InsertMessage(DB, messageJson, userId, unixmillis); err != nil {
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
		time = m.UnixMillis
		message = data["message"].(string)
		println("ID:", m.UserId, "Time:", time, "Message:", message)
	}

}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / 1e6
}
