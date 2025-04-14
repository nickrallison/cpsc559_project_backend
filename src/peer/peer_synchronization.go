package peer

import (
	"container/heap"
	"cpsc559/src/database"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"strings"
	"time"
)

func (ps *PeerServer) updatePersistedClock() {
	if err := database.SetState(ps.DB, "lamport_clock", ps.LamportClock); err != nil {
		log.Printf("Failed to persist lamport clock: %v", err)
	}
}

// pushUpdateToPeer is used by the leader to replicate a write update to a follower
// It sends the update and waits for an ACK that confirms the follower applied the update
func (ps *PeerServer) pushUpdateToPeer(addr string, pm PeerMessage) error {
	if addr == ps.PeerAddr {
		return nil
	}
	pm.Metadata.Sender = ps.PeerAddr
	log.Printf("DEBUG: Pushing update to %s: type %d, timestamp %d", addr, pm.Type, pm.Timestamp)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to follower at %s: %v", addr, err)
	}
	defer conn.Close()
	enc := json.NewEncoder(conn)
	if err := enc.Encode(pm); err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	dec := json.NewDecoder(conn)
	var ack AckMessage
	if err := dec.Decode(&ack); err != nil {
		return fmt.Errorf("failed to decode ack: %v", err)
	}

	// Determine the expected status based on the message type
	expectedStatus := "OK" // default
	switch pm.Type {
	case PrepareStoreObject, PrepareUpdateObject, PrepareDeleteObject:
		expectedStatus = "PREPARED"
	case CommitStoreObject, CommitUpdateObject, CommitDeleteObject:
		expectedStatus = "OK"
	case AbortStoreObject, AbortUpdateObject, AbortDeleteObject:
		expectedStatus = "ABORTED"
	}

	if ack.Status != expectedStatus || ack.Timestamp != pm.Timestamp {
		return fmt.Errorf("follower failed to ack message properly (got %q, expected %q)", ack.Status, expectedStatus)
	}
	return nil
}

// ForwardRequestToLeader is called by a follower when it receives a write request from a client
// It forwards the request to the leader and returns the leader's response
func (ps *PeerServer) ForwardRequestToLeader(pm PeerMessage) (map[string]interface{}, error) {
	pm.Metadata.Sender = ps.PeerAddr
	log.Printf("DEBUG: Forwarding request to leader (%s): type %d, timestamp %d", ps.LeaderAddr, pm.Type, pm.Timestamp)
	if ps.LeaderAddr == "" {
		return nil, fmt.Errorf("leader address is not set")
	}
	conn, err := net.Dial("tcp", ps.LeaderAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader at %s: %v", ps.LeaderAddr, err)
	}
	defer conn.Close()
	enc := json.NewEncoder(conn)
	if err := enc.Encode(pm); err != nil {
		return nil, fmt.Errorf("failed to encode message: %v", err)
	}
	dec := json.NewDecoder(conn)
	var resp map[string]interface{}
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}
	return resp, nil
}

// SynchronizeWithLeader is called by a follower to ensure its local state is up-to-date with the leader
// It connects to the leader, obtains the leader’s current Lamport clock and missing updates and enqueues them locally
func (ps *PeerServer) SynchronizeWithLeader() error {
	// Only run if in follower mode and a leader address is set.
	if ps.Role != Follower || ps.LeaderAddr == "" {
		return nil
	}

	// Save the follower's current (old) clock.
	oldClock := ps.LamportClock

	// --- First connection: Get the leader's state ---
	conn, err := net.DialTimeout("tcp", ps.LeaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %v", err)
	}
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	// Request the leader’s current Lamport clock.
	if err := enc.Encode(PeerMessage{Type: GetLamport}); err != nil {
		conn.Close()
		return fmt.Errorf("failed to send GetLamport request: %v", err)
	}
	leaderState := struct {
		Lamport    int64 `json:"lamport"`
		LastUpdate int64 `json:"last_update"`
	}{}
	if err := dec.Decode(&leaderState); err != nil {
		conn.Close()
		return fmt.Errorf("failed to decode leader state: %v", err)
	}
	conn.Close()

	// If already caught up, nothing needs to be done.
	if oldClock >= leaderState.Lamport {
		log.Printf("DEBUG: Already synchronized (old clock: %d, leader: %d)", oldClock, leaderState.Lamport)
		return nil
	}
	log.Printf("DEBUG: Starting synchronization (old clock: %d, leader: %d)", oldClock, leaderState.Lamport)

	// --- Second connection: Request missing updates ---
	conn, err = net.DialTimeout("tcp", ps.LeaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to reconnect to leader: %v", err)
	}
	defer conn.Close()
	enc = json.NewEncoder(conn)
	dec = json.NewDecoder(conn)

	// Request updates starting from oldClock + 1.
	syncReq := PeerMessage{
		Type: GetUpdatesSince,
		Metadata: InternalData{
			Sender:    ps.PeerAddr,
			StartTime: oldClock + 1,
		},
	}
	if err := enc.Encode(syncReq); err != nil {
		return fmt.Errorf("failed to send sync request: %v", err)
	}

	// Count the number of new (non-duplicate) updates.
	var newCount int

	// Process the updates from the leader.
	// Instead of breaking when we see an update with Timestamp equal to leaderState.Lamport,
	// we let the loop run until it naturally ends (e.g., when we hit EOF).
	for {
		var update PeerMessage
		if err := dec.Decode(&update); err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "wsarecv:") {
				break // End-of-stream.
			}
			return fmt.Errorf("failed to decode update: %v", err)
		}

		// Process only updates with timestamps in the range (oldClock, leaderState.Lamport].
		if update.Timestamp <= oldClock || update.Timestamp > leaderState.Lamport {
			continue
		}

		// Check for duplicates in the local database.
		duplicate := false
		switch update.Type {
		// For store operations, if an object with the same id exists and data matches, consider it a duplicate.
		case StoreObject, CommitStoreObject:
			if obj, err := ps.GetObject(update.Data.UserId, update.Data.UserMessageID); err == nil && obj.Data == update.Data.Data {
				duplicate = true
			}
		case UpdateObject, CommitUpdateObject:
			if obj, err := ps.GetObject(update.Data.UserId, update.Data.UserMessageID); err == nil && obj.Data == update.Data.Data {
				duplicate = true
			}
		// For delete operations, if the object is not present, assume it has already been removed.
		case DeleteObject, CommitDeleteObject:
			// Always process deletion updates, ensuring that even if the object is absent, the deletion event is applied.
			duplicate = false
		}

		// Enqueue the update only if it is not a duplicate.
		if !duplicate {
			ps.EnqueueMessage(update)
			newCount++
		}
	}

	// Explicitly update the local Lamport clock to the leader's value.
	ps.LamportClock = leaderState.Lamport
	ps.updatePersistedClock()

	log.Printf("DEBUG: Synchronization complete - %d new updates enqueued (old clock: %d, new clock: %d)", newCount, oldClock, ps.LamportClock)

	// Process all enqueued updates in order.
	ps.flushQueue()

	return nil
}

// appendToUpdateLog appends an update to the local log and prunes the log if it exceeds the maximum size
func (ps *PeerServer) appendToUpdateLog(pm PeerMessage) {
	ps.updateLogMutex.Lock()
	defer ps.updateLogMutex.Unlock()

	ps.updateLog = append(ps.updateLog, pm)

	// Prune if needed
	if len(ps.updateLog) > ps.maxLogSize {
		ps.updateLog = ps.updateLog[len(ps.updateLog)-ps.maxLogSize:]
	}
}

func (ps *PeerServer) flushQueue() {
	ps.queueLock.Lock()
	defer ps.queueLock.Unlock()
	for ps.msgQueue.Len() > 0 {
		item := heap.Pop(&ps.msgQueue).(*MessageItem)
		ps.applyMessage(item.message)
		ps.appendToUpdateLog(item.message)
		if item.message.Timestamp > ps.LamportClock {
			ps.LamportClock = item.message.Timestamp
		}
	}
}

// getUpdatesSince returns all updates in the local log with timestamps greater than the given value
func (ps *PeerServer) getUpdatesSince(since int64) ([]PeerMessage, error) {
	ps.updateLogMutex.Lock()
	defer ps.updateLogMutex.Unlock()

	var updates []PeerMessage

	// Binary search for the first update with Timestamp > since
	left := 0
	right := len(ps.updateLog) - 1
	startIdx := -1

	for left <= right {
		mid := left + (right-left)/2
		if ps.updateLog[mid].Timestamp >= since {
			startIdx = mid
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	if startIdx == -1 {
		return updates, nil // No updates found
	}

	// Return all updates from startIdx onward
	updates = make([]PeerMessage, len(ps.updateLog)-startIdx)
	copy(updates, ps.updateLog[startIdx:])

	return updates, nil
}

// getPeerTimestamp contacts a peer to obtain its current Lamport clock
func (ps *PeerServer) getPeerTimestamp(addr string) (int64, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(PeerMessage{Type: GetLamport}); err != nil {
		return 0, err
	}

	var response struct {
		Lamport int64 `json:"lamport"`
	}
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return 0, err
	}

	return response.Lamport, nil
}

// synchronizeWithPeer is used during leader reconciliation
// It contacts a peer and requests all updates since the local Lamport clock, then enqueues them
func (ps *PeerServer) synchronizeWithPeer(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Request updates since our last timestamp
	syncReq := PeerMessage{
		Type: GetUpdatesSince,
		Metadata: InternalData{
			Sender:    ps.PeerAddr,
			StartTime: ps.LamportClock + 1,
		},
	}

	if err := json.NewEncoder(conn).Encode(syncReq); err != nil {
		return err
	}

	// Process incoming updates
	dec := json.NewDecoder(conn)
	for {
		var update PeerMessage
		if err := dec.Decode(&update); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if update.Timestamp > ps.LamportClock {
			ps.EnqueueMessage(update)
			ps.LamportClock = update.Timestamp
		}
	}

	return nil
}

// EnqueueMessage adds a message to the priority queue
// The queue orders messages by their Lamport timestamps
func (ps *PeerServer) EnqueueMessage(pm PeerMessage) {
	ps.queueLock.Lock()
	defer ps.queueLock.Unlock()
	heap.Push(&ps.msgQueue, &MessageItem{message: pm})
	log.Printf("DEBUG: Enqueued message: type %d, timestamp %d. Queue length now: %d", pm.Type, pm.Timestamp, ps.msgQueue.Len())

}

// processQueue continuously processes messages from the priority queue in order
// It uses the expected timestamp to ensure messages are applied sequentially
func (ps *PeerServer) processQueue() {
	// Process messages in order; don't enforce a rigid expected timestamp.
	for {
		ps.queueLock.Lock()
		if ps.msgQueue.Len() > 0 {
			// Pop the next message (the heap is sorted by message.Timestamp)
			item := heap.Pop(&ps.msgQueue).(*MessageItem)
			ps.queueLock.Unlock()
			ps.applyMessage(item.message)
			ps.appendToUpdateLog(item.message)
			// Ensure our Lamport clock is at least as high as the message's timestamp.
			if item.message.Timestamp > ps.LamportClock {
				ps.LamportClock = item.message.Timestamp
			}
			continue
		}
		ps.queueLock.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// applyMessage applies the database operation based on the message type
func (ps *PeerServer) applyMessage(pm PeerMessage) {
	switch pm.Type {
	case StoreObject, CommitStoreObject:
		if _, err := database.InsertObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
			log.Printf("applyMessage: error storing object: %v", err)
		}
	case UpdateObject, CommitUpdateObject:
		if _, err := database.UpdateObjects(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
			log.Printf("applyMessage: error updating object: %v", err)
		}
	case DeleteObject, CommitDeleteObject:
		if _, err := database.DeleteObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID); err != nil {
			log.Printf("applyMessage: error deleting object: %v", err)
		}
	}
}

// MessageItem represents an item in the priority queue
type MessageItem struct {
	message PeerMessage
	index   int
}

// PriorityQueue implements a heap-based priority queue for MessageItem
// ordering messages by their Timestamp
type PriorityQueue []*MessageItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].message.Timestamp < pq[j].message.Timestamp
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*MessageItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func max(a, b int64) int64 {
	return int64(math.Max(float64(a), float64(b)))
}
