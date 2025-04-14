package peer

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
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

// isPeerAlive attempts to establish a TCP connection with the given address
// It returns true if the peer is reachable within the timeout
func (ps *PeerServer) isPeerAlive(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
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
	conn, err := net.DialTimeout("tcp", ps.LeaderAddr, 5*time.Second)
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
	lockResult := ps.electionMutex.TryLock()
	if !lockResult {
		log.Printf("[%s] Election already in progress, skipping.", ps.PeerAddr)
		return
	}
	//ps.inElection = true
	//defer func() { ps.inElection = false }()

	for _, addr := range ps.knownPeers {
		if ps.isPeerLeader(addr) {
			log.Printf("[%s] Found a leader at %s, aborting election.", ps.PeerAddr, addr)
			ps.LeaderAddr = addr
			ps.Role = Follower
			ps.electionMutex.Unlock()
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
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
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
		log.Printf("[%s] No higher priority peers reachable, becoming leader...", ps.PeerAddr)
		if err := ps.becomeLeader(); err != nil {
			log.Printf("[%s] Failed to become leader: %v", ps.PeerAddr, err)
		}
		ps.electionMutex.Unlock()
		return
	}

	electionResponses := make(chan bool, len(liveHigherPeers))
	for _, addr := range liveHigherPeers {
		go func(peerAddr string) {
			log.Printf("[%s] Contacting higher priority peer %s...", ps.PeerAddr, peerAddr)
			conn, err := net.DialTimeout("tcp", peerAddr, 5*time.Second)
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
	ps.electionMutex.Unlock()
}

// isPeerLeader sends a CheckLeader message to the target peer.
// Returns true if that peer responds with CheckLeaderOK
func (ps *PeerServer) isPeerLeader(addr string) bool {
	// Don’t check yourself
	if addr == ps.PeerAddr {
		return ps.Role == Leader
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
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

// announceLeadership sends a Coordinator message to a peer to announce that this node is now the leader
func (ps *PeerServer) announceLeadership(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
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
