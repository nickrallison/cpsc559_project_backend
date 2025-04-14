package peer

import (
	"bufio"
	"cpsc559/src/database"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

// PeerServer represents a node in the distributed system
// It stores information about its role, addresses, local database,
// message queues for updates, and metadata required for synchronization
type PeerServer struct {
	Role       Role         // Leader or Follower
	PeerAddr   string       // e.g. "localhost:9000"
	LeaderAddr string       // If follower, the leader’s address
	DB         *sql.DB      // Local database handle
	knownPeers []string     // For leader: a list of follower addresses
	listener   net.Listener // TCP listener for peer-to-peer communications
	//inElection           bool          // Flag to prevent concurrent elections
	LamportClock         int64         // Logical clock (Lamport clock) for ordering updates
	msgQueue             PriorityQueue // Priority queue for write updates (ordered by Lamport timestamp)
	queueLock            sync.Mutex    // Mutex for concurrent access to the queue
	updateLog            []PeerMessage // Log of applied updates (used for synchronization)
	updateLogMutex       sync.Mutex    // Mutex for concurrent access to the update log
	electionMutex        sync.Mutex    // Mutex for concurrent access to election state
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
		delay := ps.averageDelay + time.Duration(rand.NormFloat64()*float64(ps.stdevDelay))
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
			ps.StartElection()

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
