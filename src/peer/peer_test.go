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
	db, err := database.InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB error: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Insert one object.
	userId := 1
	userMsgId := 1
	text := "Data from peer read test"
	seqNum := int64(1)
	if _, err := database.InsertObjectWithSeq(db, userId, userMsgId, text, seqNum); err != nil {
		t.Fatalf("InsertObjectWithSeq error: %v", err)
	}

	// Create and start a follower peer server.
	ps := NewPeerServer(Follower, "9100", "", "", db)
	ps.Start()
	t.Cleanup(func() { ps.Stop() })

	// Simulate a client connection sending a GetObject message.
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
	if objects[0].Data != text || objects[0].SequenceNumber != seqNum {
		t.Errorf("Expected object data %q and sequence %d, got %q and %d", text, seqNum, objects[0].Data, objects[0].SequenceNumber)
	}
}

// TestPeerWriteForwarding sets up a leader and a follower peer server.
func TestPeerWriteForwarding(t *testing.T) {
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader.db")
	followerDBPath := "file:" + filepath.Join(tempDir, "follower.db")

	// Initialize Leader DB
	leaderDB, err := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader InitDB error: %v", err)
	}

	// Initialize Follower DB
	followerDB, err := database.InitDB(followerDBPath)
	if err != nil {
		t.Fatalf("Follower InitDB error: %v", err)
	}

	t.Cleanup(func() {
		leaderDB.Close()
		followerDB.Close()
	})

	// Start the leader peer server
	leader := NewPeerServer(Leader, "9200", "", "localhost:9201", leaderDB)
	leader.Start()
	t.Cleanup(func() { leader.Stop() })

	// Start the follower peer server
	follower := NewPeerServer(Follower, "9201", "localhost:9200", "", followerDB)
	follower.Start()
	t.Cleanup(func() { follower.Stop() })

	// Allow a moment for servers to start.
	time.Sleep(100 * time.Millisecond)

	// Define test data.
	userId := 2
	userMsgId := 1
	text := "Data from write forwarding test"

	// Send a StoreObject message.
	msg := PeerMessage{
		Type: StoreObject,
		Data: database.StoredObject{
			UserId:        userId,
			UserMessageID: userMsgId,
			Data:          text,
		},
	}

	resp, err := follower.ForwardRequestToLeader(msg)
	if err != nil {
		t.Fatalf("ForwardRequestToLeader error: %v", err)
	}
	if status, ok := resp["status"]; !ok || status != "OK" {
		t.Fatalf("Expected OK response from leader, got %v", resp)
	}

	time.Sleep(200 * time.Millisecond)

	// Verify leader's DB has the object with the correct sequence number.
	leaderObjs, err := database.GetObjects(leaderDB, userId)
	if err != nil {
		t.Fatalf("Leader GetObjects error: %v", err)
	}
	if len(leaderObjs) != 1 {
		t.Fatalf("Expected 1 object in leader DB, got %d", len(leaderObjs))
	}

	// Verify follower's DB has the same object and sequence number.
	followerObjs, err := database.GetObjects(followerDB, userId)
	if err != nil {
		t.Fatalf("Follower GetObjects error: %v", err)
	}
	if len(followerObjs) != 1 {
		t.Fatalf("Expected 1 object in follower DB, got %d", len(followerObjs))
	}

	seq := leaderObjs[0].SequenceNumber
	if seq == 0 {
		t.Errorf("Expected a non-zero sequence number, got %d", seq)
	}
	if leaderObjs[0].Data != text || followerObjs[0].Data != text || leaderObjs[0].SequenceNumber != followerObjs[0].SequenceNumber {
		t.Errorf("Data or sequence mismatch: leader (data: %q, seq: %d), follower (data: %q, seq: %d)",
			leaderObjs[0].Data, leaderObjs[0].SequenceNumber, followerObjs[0].Data, followerObjs[0].SequenceNumber)
	}
}
