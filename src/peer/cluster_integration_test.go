package peer_test

import (
	"cpsc559/src/database"
	"cpsc559/src/peer"
	"cpsc559/src/httpServer" 
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestHighWriteLoadReplication simulates high write load on the leader and verifies that the follower is fully synchronized.
func TestHighWriteLoadReplication(t *testing.T) {
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader.db")
	err, leaderDB := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	followerDBPath := "file:" + filepath.Join(tempDir, "follower.db")
	err, followerDB := database.InitDB(followerDBPath)
	if err != nil {
		t.Fatalf("Follower DB init error: %v", err)
	}

	leader := peer.NewPeerServer(peer.Leader, "9200", "localhost", "", "localhost:9201", leaderDB)
	leader.Start()
	

	follower := peer.NewPeerServer(peer.Follower, "9201", "localhost", "localhost:9200", "", followerDB)
	follower.Start()
	
	// Register cleanup to stop servers and close DB connections.
	t.Cleanup(func() {
		leader.Stop()
		follower.Stop()
		// Allow background goroutines to finish.
		time.Sleep(100 * time.Millisecond)
		leaderDB.Close()
		followerDB.Close()
	})

	// Issue a high volume of writes concurrently.
	const writeCount = 100
	var wg sync.WaitGroup
	for i := 1; i <= writeCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			obj := database.StoredObject{
				UserId:        1,
				UserMessageID: i,
				Data:          "data" + strconv.Itoa(i),
			}
			if _, err := leader.StoreObjects([]database.StoredObject{obj}); err != nil {
				t.Errorf("Write %d failed: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	// Allow time for replication.
	time.Sleep(2 * time.Second)

	leaderObjs, err := database.GetObjects(leaderDB, 1)
	if err != nil {
		t.Fatalf("Leader GetObjects error: %v", err)
	}
	followerObjs, err := database.GetObjects(followerDB, 1)
	if err != nil {
		t.Fatalf("Follower GetObjects error: %v", err)
	}

	if len(leaderObjs) != writeCount {
		t.Errorf("Expected %d objects in leader; got %d", writeCount, len(leaderObjs))
	}
	if len(followerObjs) != writeCount {
		t.Errorf("Expected %d objects in follower; got %d", writeCount, len(followerObjs))
	}
	// Optionally, check that each object’s data matches.
	for i := 1; i <= writeCount; i++ {
		expected := "data" + strconv.Itoa(i)
		found := false
		for _, obj := range followerObjs {
			if obj.UserMessageID == i {
				if obj.Data != expected {
					t.Errorf("Mismatch for message id %d: expected %q, got %q", i, expected, obj.Data)
				}
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Object with message id %d not found in follower", i)
		}
	}
}

// TestMissedUpdateRecovery simulates a follower missing an update (e.g. due to network delay)
// and then forces synchronization to verify the follower catches up.
func TestMissedUpdateRecovery(t *testing.T) {
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader_missed.db")
	err, leaderDB := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	followerDBPath := "file:" + filepath.Join(tempDir, "follower_missed.db")
	err, followerDB := database.InitDB(followerDBPath)
	if err != nil {
		t.Fatalf("Follower DB init error: %v", err)
	}

	leader := peer.NewPeerServer(peer.Leader, "9202", "localhost", "", "localhost:9203", leaderDB)
	leader.Start()

	follower := peer.NewPeerServer(peer.Follower, "9203", "localhost", "localhost:9202", "", followerDB)
	follower.Start()
	
	// Register cleanup: stop servers, wait briefly, then close databases.
	t.Cleanup(func() {
		leader.Stop()
		follower.Stop()
		time.Sleep(100 * time.Millisecond)
		leaderDB.Close()
		followerDB.Close()
	})

	// Send one write from leader.
	obj := database.StoredObject{
		UserId:        2,
		UserMessageID: 1,
		Data:          "delayed update",
	}
	if _, err := leader.StoreObjects([]database.StoredObject{obj}); err != nil {
		t.Fatalf("Leader store error: %v", err)
	}
	// Simulate network delay by waiting (follower may not process update immediately).
	time.Sleep(500 * time.Millisecond)
	// Force follower synchronization.
	if err := follower.SynchronizeWithLeader(); err != nil {
		t.Errorf("Follower synchronization failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	objs, err := database.GetObjects(followerDB, 2)
	if err != nil {
		t.Fatalf("Follower GetObjects error: %v", err)
	}
	if len(objs) != 1 || objs[0].Data != "delayed update" {
		t.Errorf("Follower did not recover missed update correctly, got: %+v", objs)
	}
}

// TestSimultaneousLeaderElection simulates a partition where multiple nodes start elections concurrently, 
// then "heals" the partition to check that they eventually agree on a single leader.
func TestSimultaneousLeaderElection(t *testing.T) {
	tempDir := t.TempDir()

	dbPathA := "file:" + filepath.Join(tempDir, "nodeA.db")
	err, dbA := database.InitDB(dbPathA)
	if err != nil {
		t.Fatalf("Node A DB init error: %v", err)
	}
	dbPathB := "file:" + filepath.Join(tempDir, "nodeB.db")
	err, dbB := database.InitDB(dbPathB)
	if err != nil {
		t.Fatalf("Node B DB init error: %v", err)
	}
	dbPathC := "file:" + filepath.Join(tempDir, "nodeC.db")
	err, dbC := database.InitDB(dbPathC)
	if err != nil {
		t.Fatalf("Node C DB init error: %v", err)
	}

	// Create the peer nodes.
	nodeA := peer.NewPeerServer(peer.Leader, "9204", "localhost", "", "localhost:9205,localhost:9206", dbA)
	nodeA.Start()
	nodeB := peer.NewPeerServer(peer.Follower, "9205", "localhost", "localhost:9204", "localhost:9206", dbB)
	nodeB.Start()
	nodeC := peer.NewPeerServer(peer.Follower, "9206", "localhost", "localhost:9204", "", dbC)
	nodeC.Start()

	// Register a cleanup function to stop servers and close DBs.
	t.Cleanup(func() {
		nodeA.Stop()
		nodeB.Stop()
		nodeC.Stop()
		// Allow background goroutines to finish.
		time.Sleep(100 * time.Millisecond)
		dbA.Close()
		dbB.Close()
		dbC.Close()
	})

	// Simulate a network partition: disconnect nodeB and nodeC by clearing their leader addresses.
	nodeB.LeaderAddr = ""
	nodeC.LeaderAddr = ""
	// Both nodeB and nodeC start elections concurrently.
	go nodeB.StartElection()
	go nodeC.StartElection()
	// Wait for election processes.
	time.Sleep(3 * time.Second)
	// Heal the partition: restore nodeB and nodeC leader address to nodeA.
	nodeB.LeaderAddr = nodeA.PeerAddr
	nodeC.LeaderAddr = nodeA.PeerAddr
	time.Sleep(2 * time.Second)

	// Verify all nodes agree on the same leader.
	if nodeA.Role != peer.Leader {
		t.Errorf("Node A should remain leader; role: %s", nodeA.Role.String())
	}
	if nodeB.LeaderAddr != nodeA.PeerAddr {
		t.Errorf("Node B should have Node A as leader; got %s", nodeB.LeaderAddr)
	}
	if nodeC.LeaderAddr != nodeA.PeerAddr {
		t.Errorf("Node C should have Node A as leader; got %s", nodeC.LeaderAddr)
	}
}


// TestNewLeaderReconciliation simulates a leader that crashes before fully replicating an update,
// so that the follower (upon detecting failure) becomes leader and must reconcile missing log entries.
func TestNewLeaderReconciliation(t *testing.T) {
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader_crash.db")
	err, leaderDB := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	followerDBPath := "file:" + filepath.Join(tempDir, "follower_crash.db")
	err, followerDB := database.InitDB(followerDBPath)
	if err != nil {
		t.Fatalf("Follower DB init error: %v", err)
	}

	leader := peer.NewPeerServer(peer.Leader, "9207", "localhost", "", "localhost:9208", leaderDB)
	leader.Start()
	// Do not defer leader.Stop() because we want to simulate a crash.
	follower := peer.NewPeerServer(peer.Follower, "9208", "localhost", "localhost:9207", "", followerDB)
	follower.Start()

	// Register cleanup: stop the follower, wait briefly, then close DB connections.
	t.Cleanup(func() {
		follower.Stop()
		time.Sleep(100 * time.Millisecond)
		leaderDB.Close()
		followerDB.Close()
	})

	// Leader processes a write but does NOT push it (simulate crash pre-replication).
	obj := database.StoredObject{UserId: 3, UserMessageID: 1, Data: "Write before crash"}
	leader.LamportClock++
	msg := peer.PeerMessage{Type: peer.StoreObject, Data: obj, Timestamp: leader.LamportClock}
	leader.EnqueueMessage(msg)
	// Simulate leader crash.
	leader.Stop()
	
	// Force follower election explicitly.
	follower.StartElection()
	// Wait for the follower to detect failure and complete election.
	time.Sleep(5 * time.Second)

	// Expect the follower to become leader.
	if follower.Role != peer.Leader {
		t.Errorf("Expected follower to become leader; role is %s", follower.Role.String())
	}
	// Since the leader crashed before replication, the write should not be committed.
	objs, err := database.GetObjects(followerDB, 3)
	if err != nil {
		t.Fatalf("GetObjects error: %v", err)
	}
	if len(objs) != 0 {
		t.Errorf("Expected 0 objects for user 3; got %d", len(objs))
	}
}


// TestReadConsistencyDuringTransition verifies that reads (served from followers)
// remain consistent even during leader transitions.
func TestReadConsistencyDuringTransition(t *testing.T) {
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader_read.db")
	err, leaderDB := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	followerDBPath := "file:" + filepath.Join(tempDir, "follower_read.db")
	err, followerDB := database.InitDB(followerDBPath)
	if err != nil {
		t.Fatalf("Follower DB init error: %v", err)
	}

	leader := peer.NewPeerServer(peer.Leader, "9209", "localhost", "", "localhost:9210", leaderDB)
	leader.Start()

	follower := peer.NewPeerServer(peer.Follower, "9210", "localhost", "localhost:9209", "", followerDB)
	follower.Start()

	// Register cleanup to ensure servers are stopped and databases closed.
	t.Cleanup(func() {
		leader.Stop()   // Even if already stopped later, this is safe.
		follower.Stop()
		// Allow background goroutines to finish.
		time.Sleep(100 * time.Millisecond)
		leaderDB.Close()
		followerDB.Close()
	})

	// Write an update via the leader.
	obj := database.StoredObject{UserId: 4, UserMessageID: 1, Data: "Consistent Read Test"}
	if _, err := leader.StoreObjects([]database.StoredObject{obj}); err != nil {
		t.Fatalf("Leader store error: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Simulate leader crash so that follower becomes leader.
	leader.Stop()
	time.Sleep(2 * time.Second)

	// Set up an HTTP server for the follower.
	handler := httpServer.NewHTTPHandler(&follower)
	testServer := httptest.NewServer(handler)
	defer testServer.Close()

	// Use HTTP GET to retrieve objects for user 4.
	resp, err := http.Get(fmt.Sprintf("%s/objects?userId=%d", testServer.URL, 4))
	if err != nil {
		t.Fatalf("HTTP GET error: %v", err)
	}
	defer resp.Body.Close()

	var objs []database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&objs); err != nil {
		t.Fatalf("Decoding GET response failed: %v", err)
	}
	if len(objs) != 1 || objs[0].Data != "Consistent Read Test" {
		t.Errorf("Expected consistent read after leader transition, got: %+v", objs)
	}
}
