package peer_test

import (
	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"cpsc559/src/peer"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
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

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	leader := peer.NewPeerServer(peer.Leader, "9200", "localhost", "", "localhost:9201", leaderDB, simulateDelay, avgDelay, stdevDelay)
	leader.Start()

	follower := peer.NewPeerServer(peer.Follower, "9201", "localhost", "localhost:9200", "", followerDB, simulateDelay, avgDelay, stdevDelay)
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

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	leader := peer.NewPeerServer(peer.Leader, "9202", "localhost", "", "localhost:9203", leaderDB, simulateDelay, avgDelay, stdevDelay)
	leader.Start()

	follower := peer.NewPeerServer(peer.Follower, "9203", "localhost", "localhost:9202", "", followerDB, simulateDelay, avgDelay, stdevDelay)
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

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	// Create the peer nodes.
	nodeA := peer.NewPeerServer(peer.Leader, "9204", "localhost", "", "localhost:9205,localhost:9206", dbA, simulateDelay, avgDelay, stdevDelay)
	nodeA.Start()
	nodeB := peer.NewPeerServer(peer.Follower, "9205", "localhost", "localhost:9204", "localhost:9206", dbB, simulateDelay, avgDelay, stdevDelay)
	nodeB.Start()
	nodeC := peer.NewPeerServer(peer.Follower, "9206", "localhost", "localhost:9204", "", dbC, simulateDelay, avgDelay, stdevDelay)
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

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	leader := peer.NewPeerServer(peer.Leader, "9207", "localhost", "", "localhost:9208", leaderDB, simulateDelay, avgDelay, stdevDelay)
	leader.Start()
	// Do not defer leader.Stop() because we want to simulate a crash.
	follower := peer.NewPeerServer(peer.Follower, "9208", "localhost", "localhost:9207", "", followerDB, simulateDelay, avgDelay, stdevDelay)
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

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	leader := peer.NewPeerServer(peer.Leader, "9209", "localhost", "", "localhost:9210", leaderDB, simulateDelay, avgDelay, stdevDelay)
	leader.Start()

	follower := peer.NewPeerServer(peer.Follower, "9210", "localhost", "localhost:9209", "", followerDB, simulateDelay, avgDelay, stdevDelay)
	follower.Start()

	// Register cleanup to ensure servers are stopped and databases closed.
	t.Cleanup(func() {
		leader.Stop() // Even if already stopped later, this is safe.
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

// startDummyServer starts a TCP listener on the given port that accepts connections
// but never sends an acknowledgment. It returns the listener and a channel that
// can be closed to stop the dummy server.
func startDummyServer(port string, stop chan struct{}) net.Listener {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		panic(fmt.Sprintf("Failed to start dummy server on port %s: %v", port, err))
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Simulate delay: wait longer than the leader's pushUpdateToPeer timeout (5 sec)
			go func(c net.Conn) {
				select {
				case <-stop:
					// Stop immediately if signaled.
				case <-time.After(10 * time.Second):
				}
				c.Close()
			}(conn)
		}
	}()
	return ln
}

// ------------------------
// Test Case 1: Quorum Met
// ------------------------
// All four followers are proper instances.
func TestQuorumReplicationCaseMet(t *testing.T) {
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader_met.db")
	err, leaderDB := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	// Create four follower databases.
	followerDBs := make([]*sql.DB, 4)
	followerPaths := []string{
		"file:" + filepath.Join(tempDir, "follower_met1.db"),
		"file:" + filepath.Join(tempDir, "follower_met2.db"),
		"file:" + filepath.Join(tempDir, "follower_met3.db"),
		"file:" + filepath.Join(tempDir, "follower_met4.db"),
	}
	for i, path := range followerPaths {
		err, db := database.InitDB(path)
		if err != nil {
			t.Fatalf("Follower %d DB init error: %v", i+1, err)
		}
		followerDBs[i] = db
	}

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	// Leader on port 9320.
	leader := peer.NewPeerServer(peer.Leader, "9320", "localhost", "", "localhost:9321,localhost:9322,localhost:9323,localhost:9324", leaderDB, simulateDelay, avgDelay, stdevDelay)
	leader.Start()
	// Start four follower servers on ports 9321-9324.
	followerPorts := []string{"9321", "9322", "9323", "9324"}
	followers := make([]*peer.PeerServer, len(followerPorts))
	for i, port := range followerPorts {
		ps := peer.NewPeerServer(peer.Follower, port, "localhost", "localhost:9320", "", followerDBs[i], simulateDelay, avgDelay, stdevDelay)
		ps.Start()
		followers[i] = &ps
	}

	t.Cleanup(func() {
		leader.Stop()
		for _, f := range followers {
			f.Stop()
		}
		time.Sleep(100 * time.Millisecond)
		leaderDB.Close()
		for _, db := range followerDBs {
			db.Close()
		}
	})

	// Send a write.
	obj := database.StoredObject{
		UserId:        100,
		UserMessageID: 1,
		Data:          "case1: quorum met",
	}
	if _, err := leader.StoreObjects([]database.StoredObject{obj}); err != nil {
		t.Fatalf("Expected write to succeed, but got error: %v", err)
	}

	// Allow time for replication.
	time.Sleep(2 * time.Second)

	// Verify that the object appears in the leader and all followers.
	leaderObjs, err := database.GetObjects(leaderDB, 100)
	if err != nil {
		t.Fatalf("Leader GetObjects error: %v", err)
	}
	if len(leaderObjs) != 1 {
		t.Errorf("Leader expected 1 object; got %d", len(leaderObjs))
	}
	for i, db := range followerDBs {
		objs, err := database.GetObjects(db, 100)
		if err != nil {
			t.Fatalf("Follower %d GetObjects error: %v", i+1, err)
		}
		if len(objs) != 1 {
			t.Errorf("Follower %d expected 1 object; got %d", i+1, len(objs))
		}
	}
}

// ---------------------------
// Test Case 2: Quorum Not Met
// ---------------------------
// Three of the four follower endpoints are dummy servers (nonresponsive) while one runs properly.
func TestQuorumReplicationCaseNotMet(t *testing.T) {
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader_notmet.db")
	err, leaderDB := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	// For the proper follower.
	followerDBPath := "file:" + filepath.Join(tempDir, "follower_notmet.db")
	err, properDB := database.InitDB(followerDBPath)
	if err != nil {
		t.Fatalf("Proper follower DB init error: %v", err)
	}

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	// Leader on port 9330.
	leader := peer.NewPeerServer(peer.Leader, "9330", "localhost", "", "localhost:9331,localhost:9332,localhost:9333,localhost:9334", leaderDB, simulateDelay, avgDelay, stdevDelay)
	leader.Start()

	// For ports 9331-9333, start dummy servers.
	dummyStopChans := make([]chan struct{}, 3)
	dummyListeners := make([]net.Listener, 3)
	dummyPorts := []string{"9331", "9332", "9333"}
	for i, port := range dummyPorts {
		stopChan := make(chan struct{})
		dummyStopChans[i] = stopChan
		dummyListeners[i] = startDummyServer(port, stopChan)
	}

	// For port 9334, start a proper follower.
	properFollower := peer.NewPeerServer(peer.Follower, "9334", "localhost", "localhost:9330", "", properDB, simulateDelay, avgDelay, stdevDelay)
	properFollower.Start()

	t.Cleanup(func() {
		leader.Stop()
		properFollower.Stop()
		for _, ln := range dummyListeners {
			ln.Close()
		}
		time.Sleep(100 * time.Millisecond)
		leaderDB.Close()
		properDB.Close()
	})

	// Send a write.
	obj := database.StoredObject{
		UserId:        200,
		UserMessageID: 1,
		Data:          "case2: quorum not met",
	}
	_, err = leader.StoreObjects([]database.StoredObject{obj})
	if err == nil {
		t.Errorf("Expected write to fail due to insufficient acks, but it succeeded")
	} else {
		t.Logf("Write failed as expected: %v", err)
	}

	// Verify that the write did not commit in leader or proper follower.
	leaderObjs, err := database.GetObjects(leaderDB, 200)
	if err != nil {
		t.Fatalf("Leader GetObjects error: %v", err)
	}
	if len(leaderObjs) != 0 {
		t.Errorf("Leader expected 0 objects; got %d", len(leaderObjs))
	}
	properObjs, err := database.GetObjects(properDB, 200)
	if err != nil {
		t.Fatalf("Proper follower GetObjects error: %v", err)
	}
	if len(properObjs) != 0 {
		t.Errorf("Proper follower expected 0 objects; got %d", len(properObjs))
	}
}

// ---------------------------------------------
// Test Case 3: Quorum Recovery with Delayed Follower
// ---------------------------------------------
// Initially, three followers are dummy servers while one is dummy but recovers during retries.
func TestQuorumReplicationCaseDelayedRecovery(t *testing.T) {
	tempDir := t.TempDir()
	leaderDBPath := "file:" + filepath.Join(tempDir, "leader_recovery.db")
	err, leaderDB := database.InitDB(leaderDBPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	// Proper follower for port 9343.
	followerDBPath1 := "file:" + filepath.Join(tempDir, "follower_recovery1.db")
	err, properDB := database.InitDB(followerDBPath1)
	if err != nil {
		t.Fatalf("Proper follower DB init error: %v", err)
	}
	// For the delayed follower (port 9344) we will start a dummy server and later replace it.
	followerDBPath2 := "file:" + filepath.Join(tempDir, "follower_recovery2.db")
	err, recoverableDB := database.InitDB(followerDBPath2)
	if err != nil {
		t.Fatalf("Recoverable follower DB init error: %v", err)
	}

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	// Leader on port 9340.
	leader := peer.NewPeerServer(peer.Leader, "9340", "localhost", "", "localhost:9341,localhost:9342,localhost:9343,localhost:9344", leaderDB, simulateDelay, avgDelay, stdevDelay)
	leader.Start()

	// For ports 9341 and 9342, start dummy servers.
	dummyStopChans := make([]chan struct{}, 2)
	dummyListeners := make([]net.Listener, 2)
	for i, port := range []string{"9341", "9342"} {
		stopChan := make(chan struct{})
		dummyStopChans[i] = stopChan
		dummyListeners[i] = startDummyServer(port, stopChan)
	}

	// For port 9343, start a proper follower.
	properFollower := peer.NewPeerServer(peer.Follower, "9343", "localhost", "localhost:9340", "", properDB, simulateDelay, avgDelay, stdevDelay)
	properFollower.Start()

	// For port 9344, initially start a dummy server.
	recoveryStop := make(chan struct{})
	dummyListener9344 := startDummyServer("9344", recoveryStop)

	// Note: The leader's knownPeers are fixed from initialization.
	t.Cleanup(func() {
		leader.Stop()
		properFollower.Stop()
		for _, ln := range dummyListeners {
			ln.Close()
		}
		// Also stop dummy on 9344 if still running.
		dummyListener9344.Close()
		time.Sleep(100 * time.Millisecond)
		leaderDB.Close()
		properDB.Close()
		recoverableDB.Close()
	})

	// In a separate goroutine, simulate recovery: after 1 second, close the dummy on port 9344
	// and start a proper follower there.
	go func() {
		time.Sleep(1 * time.Second)
		// Stop the dummy.
		close(recoveryStop)
		dummyListener9344.Close()
		// Start the proper follower on port 9344.
		recoveredFollower := peer.NewPeerServer(peer.Follower, "9344", "localhost", "localhost:9340", "", recoverableDB, simulateDelay, avgDelay, stdevDelay)
		recoveredFollower.Start()
		// Keep it running for the duration of the test.
	}()

	// Send a write.
	obj := database.StoredObject{
		UserId:        300,
		UserMessageID: 1,
		Data:          "case3: delayed follower recovery",
	}
	if _, err := leader.StoreObjects([]database.StoredObject{obj}); err != nil {
		t.Fatalf("Expected write to eventually succeed after recovery, but got error: %v", err)
	}

	// Allow time for replication.
	time.Sleep(3 * time.Second)

	// Verify that the write appears in leader, proper follower, and the recovered follower.
	leaderObjs, err := database.GetObjects(leaderDB, 300)
	if err != nil {
		t.Fatalf("Leader GetObjects error: %v", err)
	}
	if len(leaderObjs) != 1 {
		t.Errorf("Leader expected 1 object; got %d", len(leaderObjs))
	}
	properObjs, err := database.GetObjects(properDB, 300)
	if err != nil {
		t.Fatalf("Proper follower GetObjects error: %v", err)
	}
	if len(properObjs) != 1 {
		t.Errorf("Proper follower expected 1 object; got %d", len(properObjs))
	}
	recoveredObjs, err := database.GetObjects(recoverableDB, 300)
	if err != nil {
		t.Fatalf("Recovered follower GetObjects error: %v", err)
	}
	if len(recoveredObjs) != 1 {
		t.Errorf("Recovered follower expected 1 object; got %d", len(recoveredObjs))
	}
}
