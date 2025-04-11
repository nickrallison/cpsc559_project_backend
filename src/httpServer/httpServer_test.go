package httpServer_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// startHTTPHandlerForPeer returns a new HTTP handler that uses the supplied peer server.
func startHTTPHandlerForPeer(ps *peer.PeerServer) http.Handler {
	return httpServer.NewHTTPHandler(ps)
}

// setupHTTPServers creates two peer servers (a leader and a follower) along with two corresponding HTTP servers.
func setupHTTPServers(t *testing.T) (leaderURL, followerURL string, cleanup func()) {
	t.Helper()
	tempDir := t.TempDir()

	leaderPath := "file:" + filepath.Join(tempDir, "leader.db")
	err, leaderDB := database.InitDB(leaderPath)
	if err != nil {
		t.Fatalf("Leader DB init error: %v", err)
	}
	followerPath := "file:" + filepath.Join(tempDir, "follower.db")
	err, followerDB := database.InitDB(followerPath)
	if err != nil {
		t.Fatalf("Follower DB init error: %v", err)
	}

	// The actual peer-to-peer ports are arbitrary, these are just for testing
	leaderPort := "9205"
	followerPort := "9206"

	simulateDelay := true
	avgDelay := 700.0
	stdevDelay := 300.0

	leaderPS := peer.NewPeerServer(peer.Leader, leaderPort, "localhost", "", "localhost:"+followerPort, leaderDB, simulateDelay, avgDelay, stdevDelay)
	leaderPS.Start()
	followerPS := peer.NewPeerServer(peer.Follower, followerPort, "localhost", "localhost:"+leaderPort, "", followerDB, simulateDelay, avgDelay, stdevDelay)
	followerPS.Start()

	leaderHandler := startHTTPHandlerForPeer(&leaderPS)
	followerHandler := startHTTPHandlerForPeer(&followerPS)
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

// httpGetObjects sends a GET request to the /objects endpoint on the given serverURL using userId (and optionally userMessageId).
func httpGetObjects(t *testing.T, serverURL string, userId int, userMessageId ...int) ([]database.StoredObject, error) {
	t.Helper()
	u, err := url.Parse(serverURL + "/objects")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("userId", strconv.Itoa(userId))
	if len(userMessageId) > 0 {
		q.Set("userMessageId", strconv.Itoa(userMessageId[0]))
	}
	u.RawQuery = q.Encode()
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned status %d", u.String(), resp.StatusCode)
	}
	if len(userMessageId) > 0 {
		var obj database.StoredObject
		if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
			return nil, err
		}
		return []database.StoredObject{obj}, nil
	}
	var objs []database.StoredObject
	if err := json.NewDecoder(resp.Body).Decode(&objs); err != nil {
		return nil, err
	}
	return objs, nil
}

// httpPostObjects sends a POST request with the provided objects (JSON‐encoded) to the server’s /objects endpoint.
func httpPostObjects(t *testing.T, serverURL string, objs []database.StoredObject) (*http.Response, error) {
	t.Helper()
	bytesOut, err := json.Marshal(objs)
	if err != nil {
		return nil, err
	}
	return http.Post(serverURL+"/objects", "application/json", bytes.NewBuffer(bytesOut))
}

// httpPutObject sends a PUT request (update) with the provided object.
func httpPutObject(t *testing.T, serverURL string, obj database.StoredObject) (*http.Response, error) {
	t.Helper()
	bytesOut, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPut, serverURL+"/objects", bytes.NewBuffer(bytesOut))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return (&http.Client{}).Do(req)
}

// httpDeleteObject sends a DELETE request to the /objects endpoint for the given userId and userMessageId.
func httpDeleteObject(t *testing.T, serverURL string, userId, userMessageId int) (*http.Response, error) {
	t.Helper()
	u, err := url.Parse(serverURL + "/objects")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("userId", strconv.Itoa(userId))
	q.Set("userMessageId", strconv.Itoa(userMessageId))
	u.RawQuery = q.Encode()
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return nil, err
	}
	return (&http.Client{}).Do(req)
}

func TestPostObjectViaFollower(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	name := "WriteViaFollower"
	writeEndpoint := followerURL

	t.Run(name, func(t *testing.T) {
		objsToPost := []database.StoredObject{{UserId: 1, UserMessageID: 1, Data: "Hello from POST array"}}
		resp, err := httpPostObjects(t, writeEndpoint, objsToPost)
		if err != nil {
			t.Fatalf("POST request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST expected status 200, got %d", resp.StatusCode)
		}
		// Allow a short delay for replication.
		time.Sleep(7000 * time.Millisecond)
		// GET from both the leader and follower endpoints.
		leaderObjs, err := httpGetObjects(t, leaderURL, 1)
		if err != nil {
			t.Fatalf("GET from leader failed: %v", err)
		}
		if len(leaderObjs) != 1 {
			t.Fatalf("expected 1 object from leader; got %d", len(leaderObjs))
		}
		if leaderObjs[0].Data != objsToPost[0].Data {
			t.Errorf("leader data mismatch: expected %q, got %q", objsToPost[0].Data, leaderObjs[0].Data)
		}
		followerObjs, err := httpGetObjects(t, followerURL, 1)
		if err != nil {
			t.Fatalf("GET from follower failed: %v", err)
		}
		if len(followerObjs) != 1 {
			t.Fatalf("expected 1 object from follower; got %d", len(followerObjs))
		}
	})

}

func TestPostObjectViaLeader(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	name := "WriteViaLeader"
	writeEndpoint := leaderURL

	t.Run(name, func(t *testing.T) {
		// POST one object.
		objsToPost := []database.StoredObject{{UserId: 1, UserMessageID: 1, Data: "Hello from POST array"}}
		resp, err := httpPostObjects(t, writeEndpoint, objsToPost)
		if err != nil {
			t.Fatalf("POST request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST expected status 200, got %d", resp.StatusCode)
		}
		// Allow a short delay for replication.
		time.Sleep(7000 * time.Millisecond)
		// GET from both the leader and follower endpoints.
		leaderObjs, err := httpGetObjects(t, leaderURL, 1)
		if err != nil {
			t.Fatalf("GET from leader failed: %v", err)
		}
		if len(leaderObjs) != 1 {
			t.Fatalf("expected 1 object from leader; got %d", len(leaderObjs))
		}
		if leaderObjs[0].Data != objsToPost[0].Data {
			t.Errorf("leader data mismatch: expected %q, got %q", objsToPost[0].Data, leaderObjs[0].Data)
		}
		followerObjs, err := httpGetObjects(t, followerURL, 1)
		if err != nil {
			t.Fatalf("GET from follower failed: %v", err)
		}
		if len(followerObjs) != 1 {
			t.Fatalf("expected 1 object from follower; got %d", len(followerObjs))
		}
	})

}

func TestPostMultipleObjectsViaFollower(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	name := "MultipleWriteViaFollower"
	writeEndpoint := followerURL

	t.Run(name, func(t *testing.T) {
		objsToPost := []database.StoredObject{
			{UserId: 2, UserMessageID: 1, Data: "First object for user2"},
			{UserId: 2, UserMessageID: 2, Data: "Second object for user2"},
		}
		resp, err := httpPostObjects(t, writeEndpoint, objsToPost)
		if err != nil {
			t.Fatalf("POST request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST expected status 200, got %d", resp.StatusCode)
		}
		time.Sleep(7000 * time.Millisecond)
		leaderObjs, err := httpGetObjects(t, leaderURL, 2)
		if err != nil {
			t.Fatalf("GET from leader failed: %v", err)
		}
		if len(leaderObjs) != 2 {
			t.Fatalf("expected 2 objects from leader; got %d", len(leaderObjs))
		}
		followerObjs, err := httpGetObjects(t, followerURL, 2)
		if err != nil {
			t.Fatalf("GET from follower failed: %v", err)
		}
		if len(followerObjs) != 2 {
			t.Fatalf("expected 2 objects from follower; got %d", len(followerObjs))
		}
	})

}

func TestPostMultipleObjectsViaLeader(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	name := "MultipleWriteViaLeader"
	writeEndpoint := leaderURL

	t.Run(name, func(t *testing.T) {
		objsToPost := []database.StoredObject{
			{UserId: 2, UserMessageID: 1, Data: "First object for user2"},
			{UserId: 2, UserMessageID: 2, Data: "Second object for user2"},
		}
		resp, err := httpPostObjects(t, writeEndpoint, objsToPost)
		if err != nil {
			t.Fatalf("POST request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST expected status 200, got %d", resp.StatusCode)
		}
		time.Sleep(7000 * time.Millisecond)
		leaderObjs, err := httpGetObjects(t, leaderURL, 2)
		if err != nil {
			t.Fatalf("GET from leader failed: %v", err)
		}
		if len(leaderObjs) != 2 {
			t.Fatalf("expected 2 objects from leader; got %d", len(leaderObjs))
		}
		followerObjs, err := httpGetObjects(t, followerURL, 2)
		if err != nil {
			t.Fatalf("GET from follower failed: %v", err)
		}
		if len(followerObjs) != 2 {
			t.Fatalf("expected 2 objects from follower; got %d", len(followerObjs))
		}
	})
}

func TestGetObjectsAndSingleObject(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	objsToPost := []database.StoredObject{
		{UserId: 3, UserMessageID: 1, Data: "Message 1"},
		{UserId: 3, UserMessageID: 2, Data: "Message 2"},
	}
	resp, err := httpPostObjects(t, leaderURL, objsToPost)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST expected status 200, got %d", resp.StatusCode)
	}
	time.Sleep(7000 * time.Millisecond)

	// Test Getting all objects.
	for _, url := range []string{leaderURL, followerURL} {
		t.Run("GET_All "+url, func(t *testing.T) {
			objs, err := httpGetObjects(t, url, 3)
			if err != nil {
				t.Fatalf("GET all failed: %v", err)
			}
			if len(objs) != 2 {
				t.Fatalf("expected 2 objects; got %d", len(objs))
			}
		})
	}

	// Test Getting a single object by specifying userMessageId.
	for _, url := range []string{leaderURL, followerURL} {
		t.Run("GET_Single "+url, func(t *testing.T) {
			objs, err := httpGetObjects(t, url, 3, 2)
			if err != nil {
				t.Fatalf("GET single failed: %v", err)
			}
			if len(objs) != 1 {
				t.Fatalf("expected 1 object; got %d", len(objs))
			}
			if objs[0].UserMessageID != 2 || objs[0].Data != "Message 2" {
				t.Errorf("expected object with UserMessageID=2 and data %q; got %+v", "Message 2", objs[0])
			}
		})
	}
}

func TestPutObjectViaFollower(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	name := "PutViaFollower"
	putEndpoint := followerURL

	t.Run(name, func(t *testing.T) {
		// Insert an initial object via a POST sent to the leader.
		initObj := database.StoredObject{UserId: 4, UserMessageID: 1, Data: "Initial Data"}
		resp, err := httpPostObjects(t, leaderURL, []database.StoredObject{initObj})
		if err != nil {
			t.Fatalf("Initial POST failed: %v", err)
		}
		resp.Body.Close()
		time.Sleep(7000 * time.Millisecond)
		// Issue a PUT (update) on the designated endpoint.
		updatedObj := database.StoredObject{UserId: 4, UserMessageID: 1, Data: "Updated via PUT"}
		putResp, err := httpPutObject(t, putEndpoint, updatedObj)
		if err != nil {
			t.Fatalf("PUT request failed: %v", err)
		}
		putResp.Body.Close()
		time.Sleep(7000 * time.Millisecond)
		// GET from both leader and follower should reflect the update.
		for _, url := range []string{leaderURL, followerURL} {
			objs, err := httpGetObjects(t, url, 4)
			if err != nil {
				t.Fatalf("GET failed: %v", err)
			}
			if len(objs) != 1 {
				t.Fatalf("expected 1 object; got %d", len(objs))
			}
			if objs[0].Data != updatedObj.Data {
				t.Errorf("expected data %q; got %q", updatedObj.Data, objs[0].Data)
			}
		}
	})
}

func TestPutObjectViaLeader(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	name := "PutViaLeader"
	putEndpoint := leaderURL

	t.Run(name, func(t *testing.T) {
		// Insert an initial object via a POST sent to the leader.
		initObj := database.StoredObject{UserId: 4, UserMessageID: 1, Data: "Initial Data"}
		resp, err := httpPostObjects(t, leaderURL, []database.StoredObject{initObj})
		if err != nil {
			t.Fatalf("Initial POST failed: %v", err)
		}
		resp.Body.Close()
		time.Sleep(7000 * time.Millisecond)
		// Issue a PUT (update) on the designated endpoint.
		updatedObj := database.StoredObject{UserId: 4, UserMessageID: 1, Data: "Updated via PUT"}
		putResp, err := httpPutObject(t, putEndpoint, updatedObj)
		if err != nil {
			t.Fatalf("PUT request failed: %v", err)
		}
		putResp.Body.Close()
		time.Sleep(7000 * time.Millisecond)
		// GET from both leader and follower should reflect the update.
		for _, url := range []string{leaderURL, followerURL} {
			objs, err := httpGetObjects(t, url, 4)
			if err != nil {
				t.Fatalf("GET failed: %v", err)
			}
			if len(objs) != 1 {
				t.Fatalf("expected 1 object; got %d", len(objs))
			}
			if objs[0].Data != updatedObj.Data {
				t.Errorf("expected data %q; got %q", updatedObj.Data, objs[0].Data)
			}
		}
	})
}

func TestDeleteObjectViaFollower(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	name := "DeleteViaFollower"
	deleteEndpoint := followerURL

	t.Run(name, func(t *testing.T) {
		// Insert an object via POST to the leader.
		obj := database.StoredObject{UserId: 5, UserMessageID: 1, Data: "To be deleted"}
		resp, err := httpPostObjects(t, leaderURL, []database.StoredObject{obj})
		if err != nil {
			t.Fatalf("POST failed: %v", err)
		}
		resp.Body.Close()
		time.Sleep(7000 * time.Millisecond)
		// Confirm object appears via GET.
		objs, err := httpGetObjects(t, leaderURL, 5)
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		if len(objs) != 1 {
			t.Fatalf("expected 1 object before DELETE; got %d", len(objs))
		}
		// Issue DELETE via the designated endpoint.
		delResp, err := httpDeleteObject(t, deleteEndpoint, 5, 1)
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		delResp.Body.Close()
		time.Sleep(7000 * time.Millisecond)
		// GET from both servers should return either an error or an empty result.
		for _, url := range []string{leaderURL, followerURL} {
			got, err := httpGetObjects(t, url, 5)
			if err == nil && len(got) > 0 {
				t.Fatalf("expected 0 objects after DELETE from %s; got %d", url, len(got))
			}
		}
	})

}

func TestDeleteObjectViaLeader(t *testing.T) {
	leaderURL, followerURL, cleanup := setupHTTPServers(t)
	defer cleanup()

	name := "DeleteViaLeader"
	deleteEndpoint := leaderURL

	t.Run(name, func(t *testing.T) {
		// Insert an object via POST to the leader.
		obj := database.StoredObject{UserId: 5, UserMessageID: 1, Data: "To be deleted"}
		resp, err := httpPostObjects(t, leaderURL, []database.StoredObject{obj})
		if err != nil {
			t.Fatalf("POST failed: %v", err)
		}
		resp.Body.Close()
		time.Sleep(7000 * time.Millisecond)
		// Confirm object appears via GET.
		objs, err := httpGetObjects(t, leaderURL, 5)
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		if len(objs) != 1 {
			t.Fatalf("expected 1 object before DELETE; got %d", len(objs))
		}
		// Issue DELETE via the designated endpoint.
		delResp, err := httpDeleteObject(t, deleteEndpoint, 5, 1)
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		delResp.Body.Close()
		time.Sleep(7000 * time.Millisecond)
		// GET from both servers should return either an error or an empty result.
		for _, url := range []string{leaderURL, followerURL} {
			got, err := httpGetObjects(t, url, 5)
			if err == nil && len(got) > 0 {
				t.Fatalf("expected 0 objects after DELETE from %s; got %d", url, len(got))
			}
		}
	})

}
