package peer

import (
	"cpsc559/src/database"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// TestPeerRead starts a follower peer server, inserts data locally, and then
// sends a GetObject message to get back its data.
func TestPeerRead(t *testing.T) {
	// Create a temporary database for the follower.
	tempDir := t.TempDir()
	dbPath := "file:" + filepath.Join(tempDir, "peer_read.db")
	err, db := database.InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB error: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Insert one object.
	userId := 1
	userMsgId := 1
	text := "Data from peer read test"
	if _, err := database.InsertObject(db, userId, userMsgId, text); err != nil {
		t.Fatalf("InsertObject error: %v", err)
	}

	// Create and start a follower peer server.
	ps := NewPeerServer(Follower, "9100", "", "", db)
	ps.Start()
	t.Cleanup(func() { ps.Stop() })

	// Now simulate a client connection sending a GetObject message.
	conn, err := net.Dial("tcp", "localhost:9100")
	if err != nil {
		t.Fatalf("Failed to connect to peer server: %v", err)
	}
	defer conn.Close()

	// Prepare a GetObject message: UserMessageID=0 means all objects for userId.
	msg := PeerMessage{
		Type: GetObject,
		Data: database.StoredObject{
			UserId: userId,
		},
	}
	enc := json.NewEncoder(conn)
	if err := enc.Encode(msg); err != nil {
		t.Fatalf("Failed to send GetObject message: %v", err)
	}
	dec := json.NewDecoder(conn)
	var objects []database.StoredObject
	if err := dec.Decode(&objects); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(objects) != 1 {
		t.Fatalf("Expected 1 object, got %d", len(objects))
	}
	if objects[0].Data != text {
		t.Errorf("Expected object data %q, got %q", text, objects[0].Data)
	}
}

// TestPeerWriteForwarding sets up a leader and a follower peer server.
// It sends a StoreObject request from the follower. Because the request comes
// from an external source (its Sender field is not that of the leader), the follower
// will forward the write to its leader which will then push the update to all peers.
func TestPeerWriteForwarding(t *testing.T) {
	// Set up separate databases for leader and follower.
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader.db")
	followerDBPath := "file:" + filepath.Join(tempDir, "follower.db")
	err, leaderDB := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader InitDB error: %v", err)
	}
	err, followerDB := database.InitDB(followerDBPath)
	if err != nil {
		t.Fatalf("Follower InitDB error: %v", err)
	}
	t.Cleanup(func() {
		leaderDB.Close()
		followerDB.Close()
	})

	// Start the leader peer server on port 9200 with known follower "localhost:9201".
	leader := NewPeerServer(Leader, "9200", "", "localhost:9201", leaderDB)
	leader.Start()
	t.Cleanup(func() { leader.Stop() })

	// Start the follower peer server on port 9201 with its leader address set to "localhost:9200".
	follower := NewPeerServer(Follower, "9201", "localhost:9200", "", followerDB)
	follower.Start()
	t.Cleanup(func() { follower.Stop() })

	// Allow a moment for servers to start.
	time.Sleep(100 * time.Millisecond)

	// From the follower, send a StoreObject message (simulate an external write).
	userId := 2
	userMsgId := 1
	text := "Data from write forwarding test"
	msg := PeerMessage{
		Type: StoreObject,
		Data: database.StoredObject{
			UserId:        userId,
			UserMessageID: userMsgId,
			Data:          text,
		},
	}
	// The follower will forward this request to the leader.
	resp, err := follower.ForwardRequestToLeader(msg)
	if err != nil {
		t.Fatalf("ForwardRequestToLeader error: %v", err)
	}
	if status, ok := resp["status"]; !ok || status != "OK" {
		t.Fatalf("Expected OK response from leader, got %v", resp)
	}

	// Allow some time for the leader to push the update to both leader and follower databases.
	time.Sleep(100 * time.Millisecond)

	// Verify that the leader's DB now contains the object.
	leaderObjs, err := database.GetObjects(leaderDB, userId)
	if err != nil {
		t.Fatalf("Leader GetObjects error: %v", err)
	}
	if len(leaderObjs) != 1 {
		t.Fatalf("Expected 1 object in leader DB, got %d", len(leaderObjs))
	}

	// Verify that the follower's DB was updated via the leader-push.
	followerObjs, err := database.GetObjects(followerDB, userId)
	if err != nil {
		t.Fatalf("Follower GetObjects error: %v", err)
	}
	if len(followerObjs) != 1 {
		t.Fatalf("Expected 1 object in follower DB, got %d", len(followerObjs))
	}
	if leaderObjs[0].Data != text || followerObjs[0].Data != text {
		t.Errorf("Expected object data %q, got leader: %q, follower: %q", text, leaderObjs[0].Data, followerObjs[0].Data)
	}
}
