package httpServer_test

import (
	"bytes"
	"encoding/json"
	
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"cpsc559/src/database"
	"cpsc559/src/httpServer"
	"cpsc559/src/peer"
)

func startHTTPHandlerForPeer(ps *peer.PeerServer) http.Handler {
	return httpServer.NewHTTPHandler(ps)
}

func setupHTTPServers(t *testing.T) (leaderURL, followerURL string, cleanup func()) {
	t.Helper()
	tempDir := t.TempDir()

	leaderPath := "file:" + filepath.Join(tempDir, "leader.db")
	leaderDB, err := database.InitDB(leaderPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	followerPath := "file:" + filepath.Join(tempDir, "follower.db")
	followerDB, err := database.InitDB(followerPath)
	if err != nil {
		t.Fatalf("Follower DB init error: %v", err)
	}

	leaderPort := "9205"
	followerPort := "9206"

	leaderPS := peer.NewPeerServer(peer.Leader, leaderPort, "", "localhost:"+followerPort, leaderDB)
	leaderPS.Start()
	followerPS := peer.NewPeerServer(peer.Follower, followerPort, "localhost:"+leaderPort, "", followerDB)
	followerPS.Start()

	leaderHandler := startHTTPHandlerForPeer(leaderPS)
	followerHandler := startHTTPHandlerForPeer(followerPS)
	leaderHTTP := httptest.NewServer(leaderHandler)
	followerHTTP := httptest.NewServer(followerHandler)

	cleanup = func() {
		leaderHTTP.Close()
		followerHTTP.Close()
		leaderPS.Stop()
		followerPS.Stop()
		leaderDB.Close()
		followerDB.Close()
	}
	return leaderHTTP.URL, followerHTTP.URL, cleanup
}

func httpPostObjects(t *testing.T, serverURL string, objs []database.StoredObject) (*http.Response, error) {
	bytesOut, err := json.Marshal(objs)
	if err != nil {
		return nil, err
	}
	return http.Post(serverURL+"/objects", "application/json", bytes.NewBuffer(bytesOut))
}

func httpGetObjects(t *testing.T, serverURL string, userId int) ([]database.StoredObject, error) {
	u, _ := url.Parse(serverURL + "/objects")
	q := u.Query()
	q.Set("userId", strconv.Itoa(userId))
	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var objs []database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&objs); err != nil {
		return nil, err
	}
	return objs, nil
}

func TestPostObjectViaLeader(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	objsToPost := []database.StoredObject{{UserId: 1, UserMessageID: 1, Data: "Leader Message"}}
	resp, err := httpPostObjects(t, leaderURL, objsToPost)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()
	time.Sleep(200 * time.Millisecond)

	leaderObjs, _ := httpGetObjects(t, leaderURL, 1)
	followerObjs, _ := httpGetObjects(t, followerURL, 1)

	if len(leaderObjs) != 1 || len(followerObjs) != 1 {
		t.Fatalf("Expected 1 object each in leader and follower DB")
	}

	if leaderObjs[0].SequenceNumber != followerObjs[0].SequenceNumber {
		t.Errorf("Sequence numbers mismatch. Leader: %d, Follower: %d", leaderObjs[0].SequenceNumber, followerObjs[0].SequenceNumber)
	}
}

func TestPostObjectViaFollower(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	objsToPost := []database.StoredObject{{UserId: 2, UserMessageID: 1, Data: "Follower Message"}}
	resp, err := httpPostObjects(t, followerURL, objsToPost)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()
	time.Sleep(200 * time.Millisecond)

	leaderObjs, _ := httpGetObjects(t, leaderURL, 2)
	followerObjs, _ := httpGetObjects(t, followerURL, 2)

	if len(leaderObjs) != 1 || len(followerObjs) != 1 {
		t.Fatalf("Expected 1 object each in leader and follower DB")
	}

	if leaderObjs[0].SequenceNumber != followerObjs[0].SequenceNumber {
		t.Errorf("Sequence numbers mismatch. Leader: %d, Follower: %d", leaderObjs[0].SequenceNumber, followerObjs[0].SequenceNumber)
	}
}

func TestSequenceOrder(t *testing.T) {
	leaderURL, _, cleanup := setupHTTPServers(t)
	defer cleanup()

	objs := []database.StoredObject{
		{UserId: 3, UserMessageID: 1, Data: "First"},
		{UserId: 3, UserMessageID: 2, Data: "Second"},
	}
	_, err := httpPostObjects(t, leaderURL, objs)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	leaderObjs, _ := httpGetObjects(t, leaderURL, 3)
	if leaderObjs[0].SequenceNumber >= leaderObjs[1].SequenceNumber {
		t.Errorf("Sequence numbers out of order: %d, %d", leaderObjs[0].SequenceNumber, leaderObjs[1].SequenceNumber)
	}
}
