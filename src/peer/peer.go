package peer

import (
	"bufio"
	"container/heap"
	"cpsc559/src/database"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

// Role defines a constant type for peer roles.
type Role int

const (
	Leader Role = iota
	Follower
)

// String returns a string representation of a Role.
func (r Role) String() string {
	return [...]string{"leader", "follower"}[r]
}

// RoleFromString converts a string to a Role.
func RoleFromString(s *string) Role {
	switch *s {
	case "leader":
		return Leader
	case "follower":
		return Follower
	default:
		return -1
	}
}

// PeerServer represents a node in the distributed system
// It stores information about its role, addresses, local database,
// message queues for updates, and metadata required for synchronization
type PeerServer struct {
	Role                 Role          // Leader or Follower
	PeerAddr             string        // e.g. "localhost:9000"
	LeaderAddr           string        // If follower, the leader’s address
	DB                   *sql.DB       // Local database handle
	knownPeers           []string      // For leader: a list of follower addresses
	listener             net.Listener  // TCP listener for peer-to-peer communications
	inElection           bool          // Flag to prevent concurrent elections
	LamportClock         int64         // Logical clock (Lamport clock) for ordering updates
	msgQueue             PriorityQueue // Priority queue for write updates (ordered by Lamport timestamp)
	queueLock            sync.Mutex    // Mutex for concurrent access to the queue
	updateLog            []PeerMessage // Log of applied updates (used for synchronization)
	updateLogMutex       sync.Mutex    // Mutex for concurrent access to the update log
	maxLogSize           int           // Maximum number of updates to keep in the log,
	simulateNetworkDelay bool          // Flag to simulate network delay,
	averageDelay         time.Duration // Average delay for simulating network latency
	stdevDelay           time.Duration // Standard deviation for simulating network latency
}

// NewPeerServer creates and returns a new instance
// It sets up the peer's role, network addresses, database handle,
func NewPeerServer(role Role, port, addr string, leaderAddr, peers string, db *sql.DB, simulateDelay bool, avgDelayFloat float64, stdevDelayFloat float64) PeerServer {

	avgDelay := time.Duration(avgDelayFloat) * time.Millisecond
	stdevDelay := time.Duration(stdevDelayFloat) * time.Millisecond

	ps := PeerServer{
		Role: role,
		//PeerAddr:   os.Getenv("MYOWN") + port, //your own machine's api from tailscale
		PeerAddr:             addr + ":" + port, //your own machine's api from tailscale
		LeaderAddr:           leaderAddr,
		DB:                   db,
		LamportClock:         0,
		msgQueue:             make(PriorityQueue, 0),
		updateLog:            make([]PeerMessage, 0),
		maxLogSize:           1000, // Keep last 1000 updates,
		simulateNetworkDelay: simulateDelay,
		averageDelay:         avgDelay,
		stdevDelay:           stdevDelay,
	}
	// splits the peers string into a slice
	if peers != "" {
		ps.knownPeers = strings.Split(peers, ",")
	}
	if role == Follower {
		persistedClock, err := database.GetState(db, "lamport_clock")
		if err != nil {
			log.Printf("Error retrieving persisted lamport clock: %v", err)
		} else {
			ps.LamportClock = persistedClock
			log.Printf("Loaded persisted lamport clock: %d", ps.LamportClock)
		}
	}
	return ps
}
func (ps *PeerServer) updatePersistedClock() {
    if err := database.SetState(ps.DB, "lamport_clock", ps.LamportClock); err != nil {
         log.Printf("Failed to persist lamport clock: %v", err)
    }
}


// Start opens a TCP listener on the peer's address and begins accepting connections
// If the node is a follower, it also launches a goroutine to monitor the leader
func (ps *PeerServer) Start() {
	ln, err := net.Listen("tcp", ps.PeerAddr)
	if ps.Role == Follower {
		go ps.monitorLeader() // Continuously check if the leader is alive
	}
	if err != nil {
		log.Fatalf("Peer server failed to listen on %s: %v", ps.PeerAddr, err)
	}
	ps.listener = ln
	log.Printf("%s peer server listening on %s", strings.Title(ps.Role.String()), ps.PeerAddr)
	// Accept incoming connections in a goroutine
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// Listener closed (e.g., by Stop()) causes an error here
				return
			}
			// Process each connection concurrently
			go ps.handlePeerConnection(conn)
		}
	}()
	// Begin processing messages from the internal queue
	go ps.processQueue()
}

// Stop closes the TCP listener
func (ps *PeerServer) Stop() {
	if ps.listener != nil {
		_ = ps.listener.Close()
	}
}

// handlePeerConnection reads a JSON-encoded PeerMessage from a connection
// and dispatches it to the appropriate handler based on its type
func (ps *PeerServer) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	// delay the connection to simulate network latency
	if ps.simulateNetworkDelay {
		delay := (ps.averageDelay + time.Duration(rand.NormFloat64()*float64(ps.stdevDelay)))
		if delay < 0 {
			delay = 0
		}
		log.Printf("Simulating network delay of %v for connection from %s", delay, conn.RemoteAddr())
		time.Sleep(delay)
	}

	var pm PeerMessage
	dec := json.NewDecoder(conn)
	err := dec.Decode(&pm)
	if err != nil {
		// Handle benign errors (e.g., connection closed normally)
		if err == io.EOF {
			log.Printf("Debug: reached EOF, connection closed normally")
			return
		}
		// Check for the specific benign error
		if strings.Contains(err.Error(), "socket is not connected") {
			log.Printf("Debug: heartbeat connection closed (socket not connected)")
			return
		}
		log.Printf("Error decoding peer message: %v", err)
		return
	}
	// Dispatch based on the message type
	switch pm.Type {
	case GetObject:
		ps.handleGetObject(conn, pm)
	case StoreObject, PrepareStoreObject, CommitStoreObject, AbortStoreObject:
		ps.handleStoreObject(conn, pm)
	case UpdateObject, PrepareUpdateObject, CommitUpdateObject, AbortUpdateObject:
		ps.handleUpdateObject(conn, pm)
	case DeleteObject, PrepareDeleteObject, CommitDeleteObject, AbortDeleteObject:
		ps.handleDeleteObject(conn, pm)
	case Election:
		// Leader election: compare addresses (used as priority) to decide which node should lead
		log.Printf("[%s] Received Election message from %s", ps.PeerAddr, pm.Metadata.Sender)
		// Compare addresses to decide priority
		if ps.PeerAddr > pm.Metadata.Sender {
			// If our address is higher, respond with an ElectionAnswer
			response := PeerMessage{
				Type:     ElectionAnswer,
				Metadata: InternalData{Sender: ps.PeerAddr},
			}
			enc := json.NewEncoder(conn)
			if err := enc.Encode(response); err != nil {
				log.Printf("[%s] Error sending election answer: %v", ps.PeerAddr, err)
			}
			// Start our own election if not already in progress
			if !ps.inElection {
				log.Printf("[%s] Starting own election due to incoming Election message.", ps.PeerAddr)
				go ps.StartElection()
			}
		} else {
			// Respond even if lower priority
			response := PeerMessage{
				Type:     ElectionAnswer,
				Metadata: InternalData{Sender: ps.PeerAddr},
			}
			enc := json.NewEncoder(conn)
			if err := enc.Encode(response); err != nil {
				log.Printf("[%s] Error sending election answer: %v", ps.PeerAddr, err)
			}
		}
	case Coordinator:
		// Only accept the coordinator if the sender has higher priority
		// Assuming lexicographical ordering, a higher address means higher priority
		if pm.Metadata.Sender > ps.PeerAddr {
			ps.LeaderAddr = pm.Metadata.Sender
			ps.Role = Follower
			log.Printf("[%s] Received Coordinator message. New leader is %s", ps.PeerAddr, ps.LeaderAddr)
		} else {
			// If the coordinator sender's priority is lower than mine, ignore the coordinator
			log.Printf("[%s] Received Coordinator message from %s but retaining leadership due to higher priority.", ps.PeerAddr, pm.Metadata.Sender)
		}
	case Heartbeat:
		// For heartbeat messages, simply log receipt
		log.Printf("[%s] Received heartbeat from %s", ps.PeerAddr, pm.Metadata.Sender)
		return
	case GetLamport:
		ps.flushQueue() // Flush pending messages to update the clock
		// Return the current Lamport clock to the requester
		enc := json.NewEncoder(conn)
		resp := map[string]interface{}{
			"lamport": ps.LamportClock,
		}
		if err := enc.Encode(resp); err != nil {
			log.Printf("Error encoding lamport response: %v", err)
		}
	case GetUpdatesSince:
		// For synchronization: return all updates with timestamp greater than the requested StartTime
		log.Printf("Leader received sync request from %s for updates after %d",
			pm.Metadata.Sender, pm.Metadata.StartTime)

		// Flush any pending messages to ensure updateLog is up-to-date.
		ps.flushQueue()

		updates, err := ps.getUpdatesSince(pm.Metadata.StartTime)
		if err != nil {
			log.Printf("Error getting updates: %v", err)
			return
		}
		// Wrap the connection with a buffered writer
		bw := bufio.NewWriter(conn)
		enc := json.NewEncoder(bw)
		for _, update := range updates {
			if err := enc.Encode(update); err != nil {
				log.Printf("Failed to send update to follower: %v", err)
				return
			}
		}
		bw.Flush() // Ensure all data is sent
		log.Printf("Sent %d updates to %s", len(updates), pm.Metadata.Sender)

	case CheckLeader:
		// If this peer is the leader, return CheckLeaderOK. Otherwise, do nothing special.
		response := PeerMessage{
			Metadata: InternalData{Sender: ps.PeerAddr},
		}
		if ps.Role == Leader {
			response.Type = CheckLeaderOK
		}
		enc := json.NewEncoder(conn)
		if err := enc.Encode(response); err != nil {
			log.Printf("[%s] Error sending CheckLeader reply: %v", ps.PeerAddr, err)
		}
		return
	
	default:
		log.Printf("[%s] Unhandled peer message type: %d", ps.PeerAddr, pm.Type)
	}

}

// handleGetObject retrieves objects from the local database and sends them back
func (ps *PeerServer) handleGetObject(conn net.Conn, pm PeerMessage) {
	enc := json.NewEncoder(conn)
	if pm.Data.UserMessageID != 0 {
		// If a specific message ID is provided, fetch that single object
		row := ps.DB.QueryRow("SELECT user_id, user_message_id, data FROM objects WHERE user_id = ? AND user_message_id = ?",
			pm.Data.UserId, pm.Data.UserMessageID)
		var obj database.StoredObject
		if err := row.Scan(&obj.UserId, &obj.UserMessageID, &obj.Data); err != nil {
			log.Printf("Error scanning single object: %v", err)
			return
		}
		if err := enc.Encode(obj); err != nil {
			log.Printf("Error encoding single object: %v", err)
		}
	} else {
		// Otherwise, fetch all objects for the user
		objects, err := database.GetObjects(ps.DB, pm.Data.UserId)
		if err != nil {
			log.Printf("Error getting objects: %v", err)
			return
		}
		if err := enc.Encode(objects); err != nil {
			log.Printf("Error encoding objects: %v", err)
		}
	}
}

// handleStoreObject handles a request to store an object
// For leaders, it increments the Lamport clock, enqueues the update, and pushes it to followers
// For followers, if the message comes from the leader, it applies the update locally and sends an ACK
// Otherwise, it forwards the request to the leader
func (ps *PeerServer) handleStoreObject(conn net.Conn, pm PeerMessage) {
	enc := json.NewEncoder(conn)
	if ps.Role == Leader {
		// Leader: increment local clock, stamp the message, and enqueue it
		ps.LamportClock++              // Increment logical clock
		pm.Timestamp = ps.LamportClock // Stamp the message
		log.Printf("DEBUG: Leader storing object, new timestamp %d", pm.Timestamp)
		ps.EnqueueMessage(pm) // Enqueue update for ordered processing
		// Replicate the update to all known followers concurrently
		for _, addr := range ps.knownPeers {
			if addr == ps.PeerAddr {
				continue
			}
			go func(peerAddr string) {
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
				}
			}(addr)
		}
		// Respond with an OK status
		resp := map[string]string{"status": "OK"}
		if err := enc.Encode(resp); err != nil {
			log.Printf("Error encoding response in StoreObject (leader): %v", err)
		}
	} else if ps.Role == Follower {
		switch pm.Type {
		case PrepareStoreObject:
			// Do not apply- just respond with a prepared ack
			ack := AckMessage{
				Status:    "PREPARED",
				Timestamp: pm.Timestamp,
			}
			if err := enc.Encode(ack); err != nil {
				log.Printf("Error encoding PREPARED ack: %v", err)
			}
		case CommitStoreObject:
			// Now commit the update
			ps.LamportClock = max(ps.LamportClock, pm.Timestamp)
			ps.EnqueueMessage(pm)
			ack := AckMessage{
				Status:    "OK",
				Timestamp: pm.Timestamp,
			}
			if err := enc.Encode(ack); err != nil {
				log.Printf("Error encoding commit ack: %v", err)
			}
		case AbortStoreObject:
			// Update is aborted- simply acknowledge
			ack := AckMessage{
				Status:    "ABORTED",
				Timestamp: pm.Timestamp,
			}
			if err := enc.Encode(ack); err != nil {
				log.Printf("Error encoding abort ack: %v", err)
			}
		default:
			// For backward compatibility, if a normal StoreObject message is received:
			if pm.Metadata.Sender == ps.LeaderAddr {
				ps.LamportClock = max(ps.LamportClock, pm.Timestamp)
				ps.EnqueueMessage(pm)
				ack := AckMessage{
					Status:    "OK",
					Timestamp: pm.Timestamp,
				}
				if err := enc.Encode(ack); err != nil {
					log.Printf("Error encoding ACK in StoreObject (follower applying update): %v", err)
				}
			} else {
				// Otherwise, forward the request to the leader
				respMap, err := ps.ForwardRequestToLeader(pm)
				if err != nil {
					log.Printf("Follower failed to forward store to leader: %v", err)
					enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
					return
				}
				if err := enc.Encode(respMap); err != nil {
					log.Printf("Error encoding response in StoreObject (follower forwarding): %v", err)
				}
			}
		}
	}
}

// handleUpdateObject and handleDeleteObject follow a similar pattern:
// For leaders, update the clock, enqueue the operation, push updates to followers, and respond
// For followers, if the message is from the leader, apply the update locally and send an ACK
// otherwise, forward the request to the leader
func (ps *PeerServer) handleUpdateObject(conn net.Conn, pm PeerMessage) {
	enc := json.NewEncoder(conn)
	if ps.Role == Leader {
		ps.LamportClock++
		pm.Timestamp = ps.LamportClock

		// Phase 1: Prepare update
		prepareMsg := PeerMessage{
			Type:      PrepareUpdateObject,
			Data:      pm.Data,
			Timestamp: pm.Timestamp,
			Metadata:  InternalData{Sender: ps.PeerAddr},
		}
		if err := ps.replicateWithRetry(prepareMsg); err != nil {
			// Phase 1 failed: send abort messages
			abortMsg := PeerMessage{
				Type:      AbortUpdateObject,
				Data:      pm.Data,
				Timestamp: pm.Timestamp,
				Metadata:  InternalData{Sender: ps.PeerAddr},
			}
			for _, addr := range ps.knownPeers {
				go func(peerAddr string) {
					_ = ps.pushUpdateToPeer(peerAddr, abortMsg)
				}(addr)
			}
			resp := map[string]string{"status": "ERROR", "message": err.Error()}
			_ = enc.Encode(resp)
			return
		}

		// Phase 2: Commit update
		commitMsg := PeerMessage{
			Type:      CommitUpdateObject,
			Data:      pm.Data,
			Timestamp: pm.Timestamp,
			Metadata:  InternalData{Sender: ps.PeerAddr},
		}
		for _, addr := range ps.knownPeers {
			go func(peerAddr string) {
				_ = ps.pushUpdateToPeer(peerAddr, commitMsg)
			}(addr)
		}
		ps.EnqueueMessage(commitMsg)
		resp := map[string]string{"status": "OK"}
		_ = enc.Encode(resp)
	} else if ps.Role == Follower {
		// Expect messages coming from the leader
		if pm.Metadata.Sender == ps.LeaderAddr {
			switch pm.Type {
			case PrepareUpdateObject:
				// Do not apply - just respond with a prepared acknowledgment
				ack := AckMessage{Status: "PREPARED", Timestamp: pm.Timestamp}
				if err := enc.Encode(ack); err != nil {
					log.Printf("Error encoding update PREPARED ack: %v", err)
				}
			case CommitUpdateObject:
				// Now apply the update
				ps.LamportClock = max(ps.LamportClock, pm.Timestamp)
				ps.EnqueueMessage(pm)
				ack := AckMessage{Status: "OK", Timestamp: pm.Timestamp}
				if err := enc.Encode(ack); err != nil {
					log.Printf("Error encoding update commit ack: %v", err)
				}
			case AbortUpdateObject:
				// Acknowledge abort
				ack := AckMessage{Status: "ABORTED", Timestamp: pm.Timestamp}
				if err := enc.Encode(ack); err != nil {
					log.Printf("Error encoding update abort ack: %v", err)
				}
			default:
				// Fallback: forward to leader
				respMap, err := ps.ForwardRequestToLeader(pm)
				if err != nil {
					log.Printf("Follower failed to forward update to leader: %v", err)
					enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
					return
				}
				if err := enc.Encode(respMap); err != nil {
					log.Printf("Error encoding update response in follower forwarding: %v", err)
				}
			}
		} else {
			// Not from leader then forward
			respMap, err := ps.ForwardRequestToLeader(pm)
			if err != nil {
				log.Printf("Follower failed to forward update to leader: %v", err)
				enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
				return
			}
			if err := enc.Encode(respMap); err != nil {
				log.Printf("Error encoding update response in follower forwarding: %v", err)
			}
		}
	}
}

// handleDeleteObject processes a DeleteObject request
func (ps *PeerServer) handleDeleteObject(conn net.Conn, pm PeerMessage) {
	enc := json.NewEncoder(conn)
	if ps.Role == Leader {
		ps.LamportClock++
		pm.Timestamp = ps.LamportClock
		// Phase 1: Prepare delete
		prepareMsg := PeerMessage{
			Type:      PrepareDeleteObject,
			Data:      pm.Data,
			Timestamp: pm.Timestamp,
			Metadata:  InternalData{Sender: ps.PeerAddr},
		}
		if err := ps.replicateWithRetry(prepareMsg); err != nil {
			abortMsg := PeerMessage{
				Type:      AbortDeleteObject,
				Data:      pm.Data,
				Timestamp: pm.Timestamp,
				Metadata:  InternalData{Sender: ps.PeerAddr},
			}
			for _, addr := range ps.knownPeers {
				go func(peerAddr string) {
					_ = ps.pushUpdateToPeer(peerAddr, abortMsg)
				}(addr)
			}
			resp := map[string]string{"status": "ERROR", "message": err.Error()}
			_ = enc.Encode(resp)
			return
		}

		// Phase 2: Commit delete
		commitMsg := PeerMessage{
			Type:      CommitDeleteObject,
			Data:      pm.Data,
			Timestamp: pm.Timestamp,
			Metadata:  InternalData{Sender: ps.PeerAddr},
		}
		for _, addr := range ps.knownPeers {
			go func(peerAddr string) {
				_ = ps.pushUpdateToPeer(peerAddr, commitMsg)
			}(addr)
		}
		ps.EnqueueMessage(commitMsg)
		resp := map[string]string{"status": "OK"}
		_ = enc.Encode(resp)
	} else if ps.Role == Follower {
		if pm.Metadata.Sender == ps.LeaderAddr {
			switch pm.Type {
			case PrepareDeleteObject:
				ack := AckMessage{Status: "PREPARED", Timestamp: pm.Timestamp}
				if err := enc.Encode(ack); err != nil {
					log.Printf("Error encoding delete PREPARED ack: %v", err)
				}
			case CommitDeleteObject:
				ps.LamportClock = max(ps.LamportClock, pm.Timestamp)
				ps.EnqueueMessage(pm)
				ack := AckMessage{Status: "OK", Timestamp: pm.Timestamp}
				if err := enc.Encode(ack); err != nil {
					log.Printf("Error encoding delete commit ack: %v", err)
				}
			case AbortDeleteObject:
				ack := AckMessage{Status: "ABORTED", Timestamp: pm.Timestamp}
				if err := enc.Encode(ack); err != nil {
					log.Printf("Error encoding delete abort ack: %v", err)
				}
			default:
				// Fallback: if a normal delete message is received, apply it
				if pm.Metadata.Sender == ps.LeaderAddr {
					ps.LamportClock = max(ps.LamportClock, pm.Timestamp)
					ps.EnqueueMessage(pm)
					ack := AckMessage{Status: "OK", Timestamp: pm.Timestamp}
					if err := enc.Encode(ack); err != nil {
						log.Printf("Error encoding ACK in DeleteObject (follower applying update): %v", err)
					}
				} else {
					respMap, err := ps.ForwardRequestToLeader(pm)
					if err != nil {
						log.Printf("Follower failed to forward delete to leader: %v", err)
						enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
						return
					}
					if err := enc.Encode(respMap); err != nil {
						log.Printf("Error encoding response in DeleteObject (follower forwarding): %v", err)
					}
				}
			}
		} else {
			// Forward to leader
			respMap, err := ps.ForwardRequestToLeader(pm)
			if err != nil {
				log.Printf("Follower failed to forward delete to leader: %v", err)
				enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
				return
			}
			if err := enc.Encode(respMap); err != nil {
				log.Printf("Error encoding response in DeleteObject (follower forwarding): %v", err)
			}
		}
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
func (ps *PeerServer) ForwardRequestToLeader(pm PeerMessage) (map[string]string, error) {
	pm.Metadata.Sender = ps.PeerAddr
	log.Printf("DEBUG: Forwarding request to leader: type %d, timestamp %d", pm.Type, pm.Timestamp)
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
	var resp map[string]string
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}
	return resp, nil
}

/////////////////////////////////////////////////////////////////////////
// Helper Methods for httpServer
/////////////////////////////////////////////////////////////////////////

// GetObjects returns all objects for the given user by reading locally
func (ps *PeerServer) GetObjects(userId int) ([]database.StoredObject, error) {
	return database.GetObjects(ps.DB, userId)
}

// GetObject returns a single object (with the given message id) by reading locally
func (ps *PeerServer) GetObject(userId, userMessageId int) (database.StoredObject, error) {
	row := ps.DB.QueryRow("SELECT user_id, user_message_id, data FROM objects WHERE user_id = ? AND user_message_id = ?", userId, userMessageId)
	var obj database.StoredObject
	err := row.Scan(&obj.UserId, &obj.UserMessageID, &obj.Data)
	return obj, err
}

// StoreObjects writes an array of objects with quorum consistency
// If the node is a follower, it forwards writes to the leader
// If the node is the leader, it ensures writes are replicated to a majority of nodes before acknowledging
func (ps *PeerServer) StoreObjects(objs []database.StoredObject) (map[string]string, error) {
	if ps.Role == Follower {
		var lastResp map[string]string
		for _, obj := range objs {
			pm := PeerMessage{Type: StoreObject, Data: obj}
			resp, err := ps.ForwardRequestToLeader(pm)
			if err != nil {
				return nil, err
			}
			lastResp = resp
		}
		return lastResp, nil
	} else if ps.Role == Leader {
		for _, obj := range objs {
			ps.LamportClock++
			currentTs := ps.LamportClock
			log.Printf("DEBUG: Leader storing batch object, timestamp %d", currentTs)

			// Phase 1: Prepare
			prepareMsg := PeerMessage{
				Type:      PrepareStoreObject,
				Data:      obj,
				Timestamp: currentTs,
				Metadata:  InternalData{Sender: ps.PeerAddr},
			}
			if err := ps.replicateWithRetry(prepareMsg); err != nil {
				// Send abort messages to live followers
				abortMsg := PeerMessage{
					Type:      AbortStoreObject,
					Data:      obj,
					Timestamp: currentTs,
					Metadata:  InternalData{Sender: ps.PeerAddr},
				}
				for _, addr := range ps.knownPeers {
					go func(peerAddr string) {
						_ = ps.pushUpdateToPeer(peerAddr, abortMsg)
					}(addr)
				}
				return nil, err
			}

			// Phase 2: Commit – send commit messages
			commitMsg := PeerMessage{
				Type:      CommitStoreObject,
				Data:      obj,
				Timestamp: currentTs,
				Metadata:  InternalData{Sender: ps.PeerAddr},
			}
			for _, addr := range ps.knownPeers {
				go func(peerAddr string) {
					_ = ps.pushUpdateToPeer(peerAddr, commitMsg)
				}(addr)
			}
			// Commit locally
			ps.EnqueueMessage(commitMsg)

		}

		return map[string]string{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}

// UpdateObject and DeleteObject follow a similar replication logic as StoreObjects
// ensuring the update is applied locally and replicated to a quorum of followers
func (ps *PeerServer) UpdateObject(obj database.StoredObject) (map[string]string, error) {
	pm := PeerMessage{Type: UpdateObject, Data: obj}
	if ps.Role == Follower {
		return ps.ForwardRequestToLeader(pm)
	} else if ps.Role == Leader {
		ps.LamportClock++
		currentTs := ps.LamportClock

		// Phase 1: Prepare update
		prepareMsg := PeerMessage{
			Type:      PrepareUpdateObject,
			Data:      obj,
			Timestamp: currentTs,
			Metadata:  InternalData{Sender: ps.PeerAddr},
		}
		if err := ps.replicateWithRetry(prepareMsg); err != nil {
			// On failure, send abort messages
			abortMsg := PeerMessage{
				Type:      AbortUpdateObject,
				Data:      obj,
				Timestamp: currentTs,
				Metadata:  InternalData{Sender: ps.PeerAddr},
			}
			for _, addr := range ps.knownPeers {
				go func(peerAddr string) {
					_ = ps.pushUpdateToPeer(peerAddr, abortMsg)
				}(addr)
			}
			return nil, fmt.Errorf("failed to achieve quorum for update: %v", err)
		}

		// Phase 2: Commit update
		commitMsg := PeerMessage{
			Type:      CommitUpdateObject,
			Data:      obj,
			Timestamp: currentTs,
			Metadata:  InternalData{Sender: ps.PeerAddr},
		}
		for _, addr := range ps.knownPeers {
			go func(peerAddr string) {
				_ = ps.pushUpdateToPeer(peerAddr, commitMsg)
			}(addr)
		}
		// Commit locally
		ps.EnqueueMessage(commitMsg)
		log.Printf("DEBUG: Achieved quorum for update at timestamp %d", currentTs)
		return map[string]string{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}

// DeleteObject deletes an object with quorum consistency
func (ps *PeerServer) DeleteObject(userId int, userMessageId int) (map[string]string, error) {
	pm := PeerMessage{Type: DeleteObject, Data: database.StoredObject{UserId: userId, UserMessageID: userMessageId}}
	if ps.Role == Follower {
		return ps.ForwardRequestToLeader(pm)
	} else if ps.Role == Leader {
		ps.LamportClock++
		currentTs := ps.LamportClock
		// Phase 1: Prepare delete
		prepareMsg := PeerMessage{
			Type:      PrepareDeleteObject,
			Data:      database.StoredObject{UserId: userId, UserMessageID: userMessageId},
			Timestamp: currentTs,
			Metadata:  InternalData{Sender: ps.PeerAddr},
		}
		if err := ps.replicateWithRetry(prepareMsg); err != nil {
			// Send abort messages on failure
			abortMsg := PeerMessage{
				Type:      AbortDeleteObject,
				Data:      database.StoredObject{UserId: userId, UserMessageID: userMessageId},
				Timestamp: currentTs,
				Metadata:  InternalData{Sender: ps.PeerAddr},
			}
			for _, addr := range ps.knownPeers {
				go func(peerAddr string) {
					_ = ps.pushUpdateToPeer(peerAddr, abortMsg)
				}(addr)
			}
			return nil, fmt.Errorf("failed to achieve quorum for delete: %v", err)
		}

		// Phase 2: Commit delete
		commitMsg := PeerMessage{
			Type:      CommitDeleteObject,
			Data:      database.StoredObject{UserId: userId, UserMessageID: userMessageId},
			Timestamp: currentTs,
			Metadata:  InternalData{Sender: ps.PeerAddr},
		}
		for _, addr := range ps.knownPeers {
			go func(peerAddr string) {
				_ = ps.pushUpdateToPeer(peerAddr, commitMsg)
			}(addr)
		}
		// Commit locally
		ps.EnqueueMessage(commitMsg)
		log.Printf("DEBUG: Achieved quorum for delete at timestamp %d", currentTs)
		return map[string]string{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}

// isPeerAlive attempts to establish a TCP connection with the given address
// It returns true if the peer is reachable within the timeout
func (ps *PeerServer) isPeerAlive(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// replicateWithRetry sends the update to all live followers and retries up to 3 times
// It computes the required quorum based solely on nodes that are alive (plus the leader)
func (ps *PeerServer) replicateWithRetry(pm PeerMessage) error {
	maxRetries := 3
	var lastAckCount int

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Build a list of live followers
		livePeers := []string{}
		for _, peerAddr := range ps.knownPeers {
			if ps.isPeerAlive(peerAddr) {
				livePeers = append(livePeers, peerAddr)
			}
		}
		// Leader is always online.
		totalOnline := len(livePeers) + 1
		// Required quorum is majority of online nodes
		requiredQuorum := (totalOnline / 2) + 1

		// Start with leader's own "ack"
		ackCount := 1
		ackChan := make(chan bool, len(livePeers))
		var wg sync.WaitGroup

		// Push the update only to live peers
		for _, addr := range livePeers {
			wg.Add(1)
			go func(peerAddr string) {
				defer wg.Done()
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Attempt %d: failed to push update to follower %s: %v", attempt+1, peerAddr, err)
					ackChan <- false
				} else {
					ackChan <- true
				}
			}(addr)
		}

		// Close channel once all routines are done
		go func() {
			wg.Wait()
			close(ackChan)
		}()

		timeout := time.After(5 * time.Second)
	ackLoop:
		for {
			select {
			case ack, ok := <-ackChan:
				if !ok {
					break ackLoop
				}
				if ack {
					ackCount++
					if ackCount >= requiredQuorum {
						break ackLoop
					}
				}
			case <-timeout:
				log.Printf("Attempt %d: timeout waiting for quorum acknowledgments", attempt+1)
				break ackLoop
			}
		}
		lastAckCount = ackCount

		if ackCount >= requiredQuorum {
			log.Printf("Achieved quorum on attempt %d (%d/%d acks)", attempt+1, ackCount, requiredQuorum)
			return nil
		}
		log.Printf("Attempt %d: quorum not reached (%d/%d acks). Retrying...", attempt+1, ackCount, requiredQuorum)
		time.Sleep(100 * time.Millisecond) // Brief backoff before retrying
	}
	return fmt.Errorf("failed to achieve quorum after %d attempts: only %d of %d online nodes agreed", maxRetries, lastAckCount, len(ps.knownPeers)+1)
}

// SynchronizeWithLeader is called by a follower to ensure its local state is up-to-date with the leader
// It connects to the leader, obtains the leader’s current Lamport clock and missing updates and enqueues them locally
func (ps *PeerServer) SynchronizeWithLeader() error {
    if ps.Role != Follower || ps.LeaderAddr == "" {
        return nil
    }

    // --- First connection: Get the leader's state ---
    conn, err := net.DialTimeout("tcp", ps.LeaderAddr, 5*time.Second)
    if err != nil {
        return fmt.Errorf("failed to connect to leader: %v", err)
    }
    enc := json.NewEncoder(conn)
    dec := json.NewDecoder(conn)

    // Request leader’s current Lamport clock.
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

    // If already synchronized, nothing to do.
    if ps.LamportClock >= leaderState.Lamport {
        log.Printf("DEBUG: Already synchronized (local:%d leader:%d)", ps.LamportClock, leaderState.Lamport)
        return nil
    }
    log.Printf("DEBUG: Starting synchronization (local:%d leader:%d)", ps.LamportClock, leaderState.Lamport)

    // --- Second connection: Request missing updates ---
    conn, err = net.DialTimeout("tcp", ps.LeaderAddr, 5*time.Second)
    if err != nil {
        return fmt.Errorf("failed to reconnect to leader: %v", err)
    }
    defer conn.Close()
    enc = json.NewEncoder(conn)
    dec = json.NewDecoder(conn)

    // Request updates since (ps.LamportClock + 1)
    syncReq := PeerMessage{
        Type: GetUpdatesSince,
        Metadata: InternalData{
            Sender:    ps.PeerAddr,
            StartTime: ps.LamportClock + 1,
        },
    }
    if err := enc.Encode(syncReq); err != nil {
        return fmt.Errorf("failed to send sync request: %v", err)
    }

    var updateCount int
    batchStart := time.Now()

    // Process incoming updates.
    for {
        var update PeerMessage
        if err := dec.Decode(&update); err != nil {
            if err == io.EOF || strings.Contains(err.Error(), "wsarecv: An established connection was aborted") {
                break // end-of-stream, treat as completion
            }
            return fmt.Errorf("failed to decode update: %v", err)
        }

        // If update timestamp is not newer than our current clock, skip it.
        if update.Timestamp <= ps.LamportClock {
            log.Printf("DEBUG: Skipping outdated update (ts:%d)", update.Timestamp)
            continue
        }

        // Advance local Lamport clock regardless of duplicate status.
        ps.LamportClock = update.Timestamp

        // --- Duplicate checking ---
        // For store operations: if an object exists with identical data, skip enqueuing.
        switch update.Type {
        case StoreObject, CommitStoreObject:
            if obj, err := ps.GetObject(update.Data.UserId, update.Data.UserMessageID); err == nil {
                if obj.Data == update.Data.Data {
                    log.Printf("DEBUG: Skipping duplicate store update for user %d, msg %d (ts:%d)",
                        update.Data.UserId, update.Data.UserMessageID, update.Timestamp)
                    continue
                }
            }
        case UpdateObject, CommitUpdateObject:
            // For update: if the stored data already equals the update, skip.
            if obj, err := ps.GetObject(update.Data.UserId, update.Data.UserMessageID); err == nil {
                if obj.Data == update.Data.Data {
                    log.Printf("DEBUG: Skipping duplicate update for user %d, msg %d (ts:%d)",
                        update.Data.UserId, update.Data.UserMessageID, update.Timestamp)
                    continue
                }
            }
        case DeleteObject, CommitDeleteObject:
            // For delete: if the object is already absent, skip.
            if _, err := ps.GetObject(update.Data.UserId, update.Data.UserMessageID); err != nil {
                log.Printf("DEBUG: Skipping delete update for user %d, msg %d (ts:%d) since object not present",
                    update.Data.UserId, update.Data.UserMessageID, update.Timestamp)
                continue
            }
        }

        // Enqueue the update so the background queue processor will apply it.
        ps.EnqueueMessage(update)
        updateCount++

        if updateCount%100 == 0 || time.Since(batchStart) > time.Second {
            log.Printf("DEBUG: Sync progress - %d updates enqueued, current ts:%d", updateCount, update.Timestamp)
            batchStart = time.Now()
        }

        if update.Timestamp >= leaderState.Lamport {
            break
        }
    }

    // After enqueuing all missing updates, flush the queue so the pending updates get applied.
    ps.flushQueue()

    // Persist the new Lamport clock.
    ps.updatePersistedClock()
    log.Printf("DEBUG: Synchronization complete - %d updates enqueued and processed, new clock: %d", updateCount, ps.LamportClock)
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

// isLeaderAlive checks if the leader is reachable by sending a heartbeat
func (ps *PeerServer) isLeaderAlive() bool {
	// If we're the leader, we know we're alive
	if ps.Role == Leader {
		return true
	}
	if ps.LeaderAddr == "" {
		return false
	}
	conn, err := net.DialTimeout("tcp", ps.LeaderAddr, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	// Send a heartbeat message
	hb := PeerMessage{
		Type: Heartbeat,
		Metadata: InternalData{
			Sender: ps.PeerAddr,
		},
	}
	enc := json.NewEncoder(conn)
	if err := enc.Encode(hb); err != nil {
		return false
	}
	return true
}

// monitorLeader runs on followers, it periodically checks if the leader is alive
// If the leader is unreachable, it triggers the election process
func (ps *PeerServer) monitorLeader() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastLeaderAlive bool

	for range ticker.C {
		if ps.Role != Follower {
			return
		}

		isAlive := ps.isLeaderAlive()

		// If leader just came back online after being offline
		if isAlive && !lastLeaderAlive {
			log.Printf("Leader reconnected, synchronizing state...")
			if err := ps.SynchronizeWithLeader(); err != nil {
				log.Printf("Failed to synchronize with leader: %v", err)
			}
		}

		lastLeaderAlive = isAlive

		if !isAlive {
			log.Printf("Leader not reachable, initiating election...")
			ps.StartElection()
		}
	}
}

// StartElection initiates the leader election process
// It contacts higher priority peers (based on lexicographical ordering of addresses)
// If none respond, it calls becomeLeader()
func (ps *PeerServer) StartElection() {
	if ps.inElection {
		log.Printf("[%s] Election already in progress, skipping.", ps.PeerAddr)
		return
	}
	ps.inElection = true
	defer func() { ps.inElection = false }()

	for _, addr := range ps.knownPeers {
        if ps.isPeerLeader(addr) {
            log.Printf("[%s] Found a leader at %s, aborting election.", ps.PeerAddr, addr)
			ps.LeaderAddr = addr
        	ps.Role = Follower
            return
        }
    }
	
	// Introduce a small random delay to avoid race conditions
	jitter := time.Duration(rand.Intn(200)) * time.Millisecond
	time.Sleep(jitter)

	log.Printf("[%s] Starting election process...", ps.PeerAddr)

	// Filter out unreachable higher-priority peers
	var liveHigherPeers []string
	var updatedKnownPeers []string
	for _, addr := range ps.knownPeers {
		if addr > ps.PeerAddr { // Using lexicographical order as priority
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				log.Printf("[%s] Higher priority peer %s unreachable, skipping...", ps.PeerAddr, addr)
				continue
			}
			conn.Close()
			liveHigherPeers = append(liveHigherPeers, addr)
			updatedKnownPeers = append(updatedKnownPeers, addr)
		} else {
			updatedKnownPeers = append(updatedKnownPeers, addr)
		}
	}
	// Update knownPeers to remove unreachable nodes
	ps.knownPeers = updatedKnownPeers

	log.Printf("[%s] Found %d reachable higher priority peer(s): %v", ps.PeerAddr, len(liveHigherPeers), liveHigherPeers)

	if len(liveHigherPeers) == 0 {
		if err := ps.becomeLeader(); err != nil {
			log.Printf("[%s] Failed to become leader: %v", ps.PeerAddr, err)
		}
		return
	}

	electionResponses := make(chan bool, len(liveHigherPeers))
	for _, addr := range liveHigherPeers {
		go func(peerAddr string) {
			log.Printf("[%s] Contacting higher priority peer %s...", ps.PeerAddr, peerAddr)
			conn, err := net.DialTimeout("tcp", peerAddr, 2*time.Second)
			if err != nil {
				log.Printf("[%s] Unable to connect to peer %s: %v", ps.PeerAddr, peerAddr, err)
				electionResponses <- false
				return
			}
			defer conn.Close()
			pm := PeerMessage{
				Type: Election,
				Metadata: InternalData{
					Sender: ps.PeerAddr,
				},
			}
			enc := json.NewEncoder(conn)
			if err := enc.Encode(pm); err != nil {
				log.Printf("[%s] Failed to send Election message to %s: %v", ps.PeerAddr, peerAddr, err)
				electionResponses <- false
				return
			}
			dec := json.NewDecoder(conn)
			var resp PeerMessage
			if err := dec.Decode(&resp); err != nil {
				log.Printf("[%s] Failed to decode response from %s: %v", ps.PeerAddr, peerAddr, err)
				electionResponses <- false
				return
			}
			if resp.Type == ElectionAnswer {
				log.Printf("[%s] Received ElectionAnswer from %s", ps.PeerAddr, peerAddr)
				electionResponses <- true
			} else {
				electionResponses <- false
			}
		}(addr)
	}

	// Increase timeout to allow responses
	timeout := time.After(5 * time.Second)
	receivedAnswer := false
	for i := 0; i < len(liveHigherPeers); i++ {
		select {
		case answer := <-electionResponses:
			if answer {
				receivedAnswer = true
			}
		case <-timeout:
			log.Printf("[%s] Election timeout reached while waiting for responses.", ps.PeerAddr)
			break
		}
	}

	if receivedAnswer {
		log.Printf("[%s] Received response from higher priority peer(s), waiting for coordinator...", ps.PeerAddr)
		time.Sleep(3 * time.Second)
		if ps.LeaderAddr == "" || ps.LeaderAddr == ps.PeerAddr {
			log.Printf("[%s] No coordinator received, restarting election.", ps.PeerAddr)
			ps.StartElection()
		}
	} else {
		if err := ps.becomeLeader(); err != nil {
			log.Printf("[%s] Failed to become leader: %v", ps.PeerAddr, err)
		}
	}
}


// isPeerLeader sends a CheckLeader message to the target peer.
// Returns true if that peer responds with CheckLeaderOK
func (ps *PeerServer) isPeerLeader(addr string) bool {
    // Don’t check yourself
    if addr == ps.PeerAddr {
        return ps.Role == Leader
    }

    conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
    if err != nil {
        return false
    }
    defer conn.Close()

    // Send the CheckLeader request
    checkMsg := PeerMessage{
        Type: CheckLeader,
        Metadata: InternalData{
            Sender: ps.PeerAddr,
        },
    }
    enc := json.NewEncoder(conn)
    if err := enc.Encode(checkMsg); err != nil {
        return false
    }

    // Wait for response
    dec := json.NewDecoder(conn)
    var resp PeerMessage
    if err := dec.Decode(&resp); err != nil {
        return false
    }

    return resp.Type == CheckLeaderOK
}


// becomeLeader transitions the node to the leader role
// It processes all pending messages, synchronizes state if needed
// Then announces leadership to its known peers
func (ps *PeerServer) becomeLeader() error {
	// Phase 1: Prepare for leadership transition
	ps.queueLock.Lock()
	defer ps.queueLock.Unlock()

	// 1. Ensure all pending messages are processed
	for ps.msgQueue.Len() > 0 {
		item := heap.Pop(&ps.msgQueue).(*MessageItem)
		ps.applyMessage(item.message)
		ps.LamportClock = item.message.Timestamp
	}

	// Phase 2: Verify we have the latest state
	var (
		highestTimestamp int64
		mostUpdatedPeer  string
	)

	// Check with all known peers to find the most up-to-date state
	for _, addr := range ps.knownPeers {
		if addr == ps.PeerAddr {
			continue // don't connect to yourself
		}
		ts, err := ps.getPeerTimestamp(addr)
		if err != nil {
			log.Printf("Failed to get timestamp from %s: %v", addr, err)
			continue
		}

		if ts > highestTimestamp {
			highestTimestamp = ts
			mostUpdatedPeer = addr
		}
	}

	// Phase 3: Synchronize state if needed
	if highestTimestamp > ps.LamportClock {
		log.Printf("Need to synchronize with %s (their ts:%d vs our ts:%d)",
			mostUpdatedPeer, highestTimestamp, ps.LamportClock)

		if err := ps.synchronizeWithPeer(mostUpdatedPeer); err != nil {
			return fmt.Errorf("failed to synchronize with %s: %v", mostUpdatedPeer, err)
		}
	}

	// Phase 4: Formalize leadership
	ps.Role = Leader
	ps.LeaderAddr = ""

	// Phase 5: Announce leadership with our state
	var wg sync.WaitGroup
	for _, addr := range ps.knownPeers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			ps.announceLeadership(peerAddr)
		}(addr)
	}

	// Wait for announcements to complete (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("Leadership announcement completed")
	case <-time.After(5 * time.Second):
		log.Printf("Timeout waiting for leadership announcements")
	}

	return nil
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
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
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

// announceLeadership sends a Coordinator message to a peer to announce that this node is now the leader
func (ps *PeerServer) announceLeadership(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	leaderMsg := PeerMessage{
		Type: Coordinator,
		Metadata: InternalData{
			Sender: ps.PeerAddr,
		},
		Timestamp: ps.LamportClock,
	}

	return json.NewEncoder(conn).Encode(leaderMsg)
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
