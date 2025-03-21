package peer

import (
	"cpsc559/src/database"
	"container/heap"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"strings"
	"time"
	"io"
	"math/rand"

	"sync"

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

// PeerServer defines an instance of a peer (leader or follower).
type PeerServer struct {
	Role       Role         // Leader or Follower
	PeerAddr   string       // e.g. "localhost:9000"
	LeaderAddr string       // If follower, the leader’s address
	DB         *sql.DB      // Local database handle
	knownPeers []string     // For leader: a list of follower peer addresses
	listener   net.Listener // the TCP listener so we can stop the server
	inElection bool         // flag to prevent concurrent elections
	lamportClock int64 
	msgQueue  PriorityQueue
	queueLock sync.Mutex
}


// NewPeerServer creates and returns a new instance.
func NewPeerServer(role Role, port, addr string, leaderAddr, peers string, db *sql.DB) PeerServer {
	ps := PeerServer{
		Role: role,
		//PeerAddr:   os.Getenv("MYOWN") + port, //your own machine's api from tailscale
		PeerAddr:   addr + ":" + port, //your own machine's api from tailscale
		LeaderAddr: leaderAddr,
		DB:         db,
		lamportClock: 0,
		msgQueue:     make(PriorityQueue, 0),
	}
	if peers != "" {
		ps.knownPeers = strings.Split(peers, ",")
	}
	return ps
}

// Start begins listening for incoming peer messages.
func (ps *PeerServer) Start() {
	ln, err := net.Listen("tcp", ps.PeerAddr)
	if ps.Role == Follower {
		go ps.monitorLeader()
	}
	if err != nil {
		log.Fatalf("Peer server failed to listen on %s: %v", ps.PeerAddr, err)
	}
	ps.listener = ln
	log.Printf("%s peer server listening on %s", strings.Title(ps.Role.String()), ps.PeerAddr)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// When Stop() closes the listener, Accept returns an error.
				return
			}
			go ps.handlePeerConnection(conn)
		}
	}()
	// msg queue processing begins
	go ps.processQueue()
}

// Stop shuts down the peer server listener.
func (ps *PeerServer) Stop() {
	if ps.listener != nil {
		_ = ps.listener.Close()
	}
}

// handlePeerConnection decodes the incoming PeerMessage and dispatches to specific handlers.
func (ps *PeerServer) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	var pm PeerMessage
	dec := json.NewDecoder(conn)
	if err := dec.Decode(&pm); err != nil {
		// If the connection is closed normally, io.EOF is expected.
		if err == io.EOF {
			// Optionally log at a debug level or ignore.
			log.Printf("Debug: reached EOF, connection closed normally")
			return
		}
		log.Printf("Error decoding peer message: %v", err)
		return
	}

	switch pm.Type {
	case GetObject:
		ps.handleGetObject(conn, pm)
	case StoreObject:
		ps.handleStoreObject(conn, pm)
	case UpdateObject:
		ps.handleUpdateObject(conn, pm)
	case DeleteObject:
		ps.handleDeleteObject(conn, pm)
	case Election:
		log.Printf("[%s] Received Election message from %s", ps.PeerAddr, pm.Metadata.Sender)
		// Compare addresses to decide priority.
		if ps.PeerAddr > pm.Metadata.Sender {
			response := PeerMessage{
				Type: ElectionAnswer,
				Metadata: InternalData{Sender: ps.PeerAddr},
			}
			enc := json.NewEncoder(conn)
			if err := enc.Encode(response); err != nil {
				log.Printf("[%s] Error sending election answer: %v", ps.PeerAddr, err)
			}
			// If not already in election, start our own.
			if !ps.inElection {
				log.Printf("[%s] Starting own election due to incoming Election message.", ps.PeerAddr)
				go ps.startElection()
			}
		} else {
			// Even if lower, respond with an ElectionAnswer.
			response := PeerMessage{
				Type: ElectionAnswer,
				Metadata: InternalData{Sender: ps.PeerAddr},
			}
			enc := json.NewEncoder(conn)
			if err := enc.Encode(response); err != nil {
				log.Printf("[%s] Error sending election answer: %v", ps.PeerAddr, err)
			}
		}
	case Coordinator:
		// If the coordinator message is from ourselves, ignore it.
		if pm.Metadata.Sender == ps.PeerAddr {
			log.Printf("[%s] Received Coordinator message from myself, ignoring.", ps.PeerAddr)
		} else {
			// Otherwise, update our leader info and become follower.
			ps.LeaderAddr = pm.Metadata.Sender
			ps.Role = Follower
			log.Printf("[%s] Received Coordinator message. New leader is %s", ps.PeerAddr, ps.LeaderAddr)
		}
	
	default:
		log.Printf("[%s] Unhandled peer message type: %d", ps.PeerAddr, pm.Type)
	}
}


// handleGetObject processes a GetObject request.
func (ps *PeerServer) handleGetObject(conn net.Conn, pm PeerMessage) {
	enc := json.NewEncoder(conn)
	if pm.Data.UserMessageID != 0 {
		// Return a single object.
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
		// Return all objects for the given user.
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

// handleStoreObject processes a StoreObject request.
func (ps *PeerServer) handleStoreObject(conn net.Conn, pm PeerMessage) {
	enc := json.NewEncoder(conn)
	if ps.Role == Leader {
		// Leader stores the object.
		// if _, err := database.InsertObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
		// 	log.Printf("Leader failed to store object: %v", err)
		// }
		// Leader: increment local clock, stamp the message, and enqueue it.
		ps.lamportClock++
		pm.Timestamp = ps.lamportClock
		log.Printf("DEBUG: Leader storing object, new timestamp %d", pm.Timestamp)
		ps.enqueueMessage(pm)
		// Then push the update to all known followers.
		for _, addr := range ps.knownPeers {
			go func(peerAddr string) {
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
				}
			}(addr)
		}
		// Send OK response.
		resp := map[string]string{"status": "OK"}
		if err := enc.Encode(resp); err != nil {
			log.Printf("Error encoding response in StoreObject (leader): %v", err)
		}
	} else if ps.Role == Follower {
		// When acting as follower, check whether the request came from the leader.
		if pm.Metadata.Sender == ps.LeaderAddr {
			// This update is coming from the leader; apply the update locally, update clock and enqueue.
			// if _, err := database.InsertObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
			// 	log.Printf("Follower failed to store object (from leader): %v", err)
			// 	enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
			// 	return
			// }
			ps.lamportClock = max(ps.lamportClock, pm.Timestamp) + 1
			log.Printf("DEBUG: Follower applying store from leader; updated clock to %d", ps.lamportClock)
			ps.enqueueMessage(pm)
			resp := map[string]string{"status": "OK"}
			if err := enc.Encode(resp); err != nil {
				log.Printf("Error encoding response in StoreObject (follower applying update): %v", err)
			}
		} else {
			// Forward the store request to the leader.
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

// handleUpdateObject processes an UpdateObject request.
func (ps *PeerServer) handleUpdateObject(conn net.Conn, pm PeerMessage) {
	enc := json.NewEncoder(conn)
	if ps.Role == Leader {
		// if _, err := database.UpdateObjects(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
		// 	log.Printf("Leader failed to update object: %v", err)
		// }
		ps.lamportClock++
		pm.Timestamp = ps.lamportClock
		log.Printf("DEBUG: Leader updating object, new timestamp %d", pm.Timestamp)
		ps.enqueueMessage(pm)
		for _, addr := range ps.knownPeers {
			go func(peerAddr string) {
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
				}
			}(addr)
		}
		resp := map[string]string{"status": "OK"}
		if err := enc.Encode(resp); err != nil {
			log.Printf("Error encoding response in UpdateObject (leader): %v", err)
		}
	} else if ps.Role == Follower {
		if pm.Metadata.Sender == ps.LeaderAddr {
			// Update coming directly from the leader; apply locally.
			// if _, err := database.UpdateObjects(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
			// 	log.Printf("Follower failed to update object (from leader): %v", err)
			// 	enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
			// 	return
			// }
			ps.lamportClock = max(ps.lamportClock, pm.Timestamp) + 1
			log.Printf("DEBUG: Follower updating object from leader; updated clock to %d", ps.lamportClock)
			ps.enqueueMessage(pm)
			resp := map[string]string{"status": "OK"}
			if err := enc.Encode(resp); err != nil {
				log.Printf("Error encoding response in UpdateObject (follower applying update): %v", err)
			}
		} else {
			// Forward the update request to the leader.
			respMap, err := ps.ForwardRequestToLeader(pm)
			if err != nil {
				log.Printf("Follower failed to forward update to leader: %v", err)
				enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
				return
			}
			if err := enc.Encode(respMap); err != nil {
				log.Printf("Error encoding response in UpdateObject (follower forwarding): %v", err)
			}
		}
	}
}

// handleDeleteObject processes a DeleteObject request.
func (ps *PeerServer) handleDeleteObject(conn net.Conn, pm PeerMessage) {
	enc := json.NewEncoder(conn)
	if ps.Role == Leader {
		ps.lamportClock++
		pm.Timestamp = ps.lamportClock
		log.Printf("DEBUG: Leader deleting object, new timestamp %d", pm.Timestamp)
		ps.enqueueMessage(pm)
		// if _, err := database.DeleteObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID); err != nil {
		// 	log.Printf("Leader failed to delete object: %v", err)
		// }
		for _, addr := range ps.knownPeers {
			go func(peerAddr string) {
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Leader failed to push delete to follower %s: %v", peerAddr, err)
				}
			}(addr)
		}
		resp := map[string]string{"status": "OK"}
		if err := enc.Encode(resp); err != nil {
			log.Printf("Error encoding response in DeleteObject (leader): %v", err)
		}
	} else if ps.Role == Follower {
		if pm.Metadata.Sender == ps.LeaderAddr {
			// Delete command coming from the leader: apply it locally.
			// if _, err := database.DeleteObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID); err != nil {
			// 	log.Printf("Follower failed to delete object (from leader): %v", err)
			// 	enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
			// 	return
			// }
			ps.lamportClock = max(ps.lamportClock, pm.Timestamp) + 1
			log.Printf("DEBUG: Follower deleting object from leader; updated clock to %d", ps.lamportClock)
			ps.enqueueMessage(pm)
			resp := map[string]string{"status": "OK"}
			if err := enc.Encode(resp); err != nil {
				log.Printf("Error encoding response in DeleteObject (follower applying update): %v", err)
			}
		} else {
			// Forward the delete request to the leader.
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

// pushUpdateToPeer is used by the leader to send write updates to a follower.
func (ps *PeerServer) pushUpdateToPeer(addr string, pm PeerMessage) error {
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
	var resp map[string]string
	if err := dec.Decode(&resp); err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}
	return nil
}

// ForwardRequestToLeader is called by a follower to send a write (store/update/delete)
// to its leader.
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

// GetObjects returns all objects for the given user by reading locally.
func (ps *PeerServer) GetObjects(userId int) ([]database.StoredObject, error) {
	return database.GetObjects(ps.DB, userId)
}

// GetObject returns a single object (with the given message id) by reading locally.
func (ps *PeerServer) GetObject(userId, userMessageId int) (database.StoredObject, error) {
	row := ps.DB.QueryRow("SELECT user_id, user_message_id, data FROM objects WHERE user_id = ? AND user_message_id = ?", userId, userMessageId)
	var obj database.StoredObject
	err := row.Scan(&obj.UserId, &obj.UserMessageID, &obj.Data)
	return obj, err
}

// StoreObjects writes an array of objects.
// If the node is a follower, it automatically forwards the writes to its leader; if leader, it writes locally and pushes updates.
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
			ps.lamportClock++
			currentTs := ps.lamportClock
			pm := PeerMessage{Type: StoreObject, Data: obj, Timestamp: currentTs}
			log.Printf("DEBUG: Leader storing batch object, timestamp %d", currentTs)
			ps.enqueueMessage(pm)
			// if _, err := database.InsertObject(ps.DB, obj.UserId, obj.UserMessageID, obj.Data); err != nil {
			// 	return nil, err
			// }
			for _, addr := range ps.knownPeers {
				go func(peerAddr string, o database.StoredObject, ts int64) {
					pm := PeerMessage{Type: StoreObject, Data: o,Timestamp: ts}
					if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
						log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
					}
				}(addr, obj, currentTs)
			}
		}
		return map[string]string{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}

// UpdateObject updates an object.
func (ps *PeerServer) UpdateObject(obj database.StoredObject) (map[string]string, error) {
	pm := PeerMessage{Type: UpdateObject, Data: obj}
	if ps.Role == Follower {
		return ps.ForwardRequestToLeader(pm)
	} else if ps.Role == Leader {
		ps.lamportClock++
		currentTs := ps.lamportClock
		pm := PeerMessage{Type: UpdateObject, Data: obj, Timestamp: currentTs}
		log.Printf("DEBUG: Leader updating batch object, timestamp %d", currentTs)
		ps.enqueueMessage(pm)
		// if _, err := database.UpdateObjects(ps.DB, obj.UserId, obj.UserMessageID, obj.Data); err != nil {
		// 	return nil, err
		// }
		for _, addr := range ps.knownPeers {
			go func(peerAddr string, o database.StoredObject,ts int64) {
				pm := PeerMessage{Type: UpdateObject, Data: o, Timestamp: ts}
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
				}
			}(addr, obj,currentTs)
		}
		return map[string]string{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}

// DeleteObject deletes an object.
func (ps *PeerServer) DeleteObject(userId int, userMessageId int) (map[string]string, error) {
	pm := PeerMessage{Type: DeleteObject, Data: database.StoredObject{UserId: userId, UserMessageID: userMessageId}}
	if ps.Role == Follower {
		return ps.ForwardRequestToLeader(pm)
	} else if ps.Role == Leader {
		ps.lamportClock++
		currentTs := ps.lamportClock
		pm := PeerMessage{Type: DeleteObject, Data: database.StoredObject{UserId: userId, UserMessageID: userMessageId}, Timestamp: currentTs}
		log.Printf("DEBUG: Leader updating batch object, timestamp %d", currentTs)
		ps.enqueueMessage(pm)
		// if _, err := database.DeleteObject(ps.DB, userId, userMessageId); err != nil {
		// 	return nil, err
		// }
		for _, addr := range ps.knownPeers {
			go func(peerAddr string, uid, umid int, ts int64) {
				pm := PeerMessage{Type: DeleteObject, Data: database.StoredObject{UserId: uid, UserMessageID: umid}, Timestamp: ts}
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Leader failed to push delete to follower %s: %v", peerAddr, err)
				}
			}(addr, userId, userMessageId,currentTs)
		}
		return map[string]string{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}



func (ps *PeerServer) isLeaderAlive() bool {
	// If we're the leader, we know we're alive.
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
	conn.Close()
	return true
}



func (ps *PeerServer) monitorLeader() {
	// Only followers need to check the leader.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		// If we are no longer a follower, stop monitoring.
		if ps.Role != Follower {
			log.Printf("[%s] No longer a follower. Stopping leader monitor.", ps.PeerAddr)
			return
		}
		log.Printf("[%s] Checking if leader (%s) is alive...", ps.PeerAddr, ps.LeaderAddr)
		if ps.LeaderAddr == "" || !ps.isLeaderAlive() {
			log.Printf("[%s] Leader %s not reachable. Initiating election...", ps.PeerAddr, ps.LeaderAddr)
			ps.startElection()
		} else {
			log.Printf("[%s] Leader %s is alive.", ps.PeerAddr, ps.LeaderAddr)
		}
	}
}


func (ps *PeerServer) startElection() {
    if ps.inElection {
        log.Printf("[%s] Election already in progress, skipping.", ps.PeerAddr)
        return
    }
    ps.inElection = true
    defer func() { ps.inElection = false }()
    
    // Introduce a small random delay to avoid race conditions.
    jitter := time.Duration(rand.Intn(200)) * time.Millisecond
    time.Sleep(jitter)
    
    log.Printf("[%s] Starting election process...", ps.PeerAddr)

    // Contact all peers in knownPeers that have a higher priority.
    var higherPeers []string
    for _, addr := range ps.knownPeers {
        if addr > ps.PeerAddr { // lexicographical comparison as proxy for priority
            higherPeers = append(higherPeers, addr)
        }
    }
    log.Printf("[%s] Found %d higher priority peer(s): %v", ps.PeerAddr, len(higherPeers), higherPeers)

    // If no higher peers exist, become leader immediately.
    if len(higherPeers) == 0 {
        ps.becomeLeader()
        return
    }

    electionResponses := make(chan bool, len(higherPeers))
    for _, addr := range higherPeers {
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

    // Increase timeout here to allow responses to come in.
    timeout := time.After(5 * time.Second)
    receivedAnswer := false
    for i := 0; i < len(higherPeers); i++ {
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
        // Wait a bit longer for a coordinator message.
        time.Sleep(3 * time.Second)
        if ps.LeaderAddr == "" || ps.LeaderAddr == ps.PeerAddr {
            log.Printf("[%s] No coordinator received, restarting election.", ps.PeerAddr)
            ps.startElection()
        }
    } else {
        ps.becomeLeader()
    }
}

func (ps *PeerServer) becomeLeader() {
	ps.Role = Leader
	ps.LeaderAddr = ps.PeerAddr
	log.Printf("[%s] No higher priority peer responded. I am the new leader.", ps.PeerAddr)
	// Broadcast the new leadership to all known peers.
	for _, addr := range ps.knownPeers {
		go func(peerAddr string) {
			log.Printf("[%s] Notifying peer %s of new leadership...", ps.PeerAddr, peerAddr)
			conn, err := net.DialTimeout("tcp", peerAddr, 2*time.Second)
			if err != nil {
				log.Printf("[%s] Failed to connect to peer %s: %v", ps.PeerAddr, peerAddr, err)
				return
			}
			defer conn.Close()
			pm := PeerMessage{
				Type: Coordinator,
				Metadata: InternalData{
					Sender: ps.PeerAddr,
				},
			}
			enc := json.NewEncoder(conn)
			if err := enc.Encode(pm); err != nil {
				log.Printf("[%s] Failed to send coordinator to %s: %v", ps.PeerAddr, peerAddr, err)
			}
		}(addr)
	}
}



=======
func (ps *PeerServer) enqueueMessage(pm PeerMessage) {
	ps.queueLock.Lock()
	defer ps.queueLock.Unlock()
	heap.Push(&ps.msgQueue, &MessageItem{message: pm})
	log.Printf("DEBUG: Enqueued message: type %d, timestamp %d. Queue length now: %d", pm.Type, pm.Timestamp, ps.msgQueue.Len())

}

// processQueue continuously processes messages from the queue in order.
func (ps *PeerServer) processQueue() {
	var expectedTimestamp int64 = 1
	for {
		ps.queueLock.Lock()
		if ps.msgQueue.Len() > 0 {
			item := ps.msgQueue[0]
			if item.message.Timestamp == expectedTimestamp {
				heap.Pop(&ps.msgQueue)
				ps.queueLock.Unlock()
				ps.applyMessage(item.message)
				expectedTimestamp++
				continue
			}
		}
		ps.queueLock.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}


// applyMessage applies the database operation based on the message type.
func (ps *PeerServer) applyMessage(pm PeerMessage) {
	switch pm.Type {
	case StoreObject:
		if _, err := database.InsertObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
			log.Printf("applyMessage: error storing object: %v", err)
		}
	case UpdateObject:
		if _, err := database.UpdateObjects(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
			log.Printf("applyMessage: error updating object: %v", err)
		}
	case DeleteObject:
		if _, err := database.DeleteObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID); err != nil {
			log.Printf("applyMessage: error deleting object: %v", err)
		}
	}
}

// MessageItem represents an item in the priority queue.
type MessageItem struct {
	message PeerMessage
	index   int
}

// PriorityQueue implements heap.Interface for MessageItem based on the timestamp.
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

