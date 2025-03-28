package peer

import (
	"container/heap"
	"cpsc559/src/database"
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
	Role       		Role         	// Leader or Follower
	PeerAddr   		string       	// e.g. "localhost:9000"
	LeaderAddr 		string       	// If follower, the leader’s address
	DB         		*sql.DB      	// Local database handle
	knownPeers 		[]string     	// For leader: a list of follower peer addresses
	listener   		net.Listener 	// the TCP listener so we can stop the server
	inElection 		bool         	// flag to prevent concurrent elections
	LamportClock 	int64 
	msgQueue  		PriorityQueue
	queueLock 		sync.Mutex
	updateLog 		[]PeerMessage
    updateLogMutex 	sync.Mutex
    maxLogSize 		int 			// Maximum number of updates to keep
}


// NewPeerServer creates and returns a new instance.
func NewPeerServer(role Role, port, addr string, leaderAddr, peers string, db *sql.DB) PeerServer {
	ps := PeerServer{
		Role: role,
		//PeerAddr:   os.Getenv("MYOWN") + port, //your own machine's api from tailscale
		PeerAddr:     addr + ":" + port, //your own machine's api from tailscale
		LeaderAddr:   leaderAddr,
		DB:           db,
		LamportClock: 0,
		msgQueue:     make(PriorityQueue, 0),
		updateLog:     make([]PeerMessage, 0),
        maxLogSize:    1000, // Keep last 1000 updates
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
    err := dec.Decode(&pm)
    if err != nil {
        // If the connection is closed normally, io.EOF is expected.
        if err == io.EOF {
            log.Printf("Debug: reached EOF, connection closed normally")
            return
        }
        // Check for the specific benign error.
        if strings.Contains(err.Error(), "socket is not connected") {
            log.Printf("Debug: heartbeat connection closed (socket not connected)")
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
				go ps.StartElection()
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
		// Only accept the coordinator if the sender has higher priority.
		// Assuming lexicographical ordering, a higher address means higher priority.
		if pm.Metadata.Sender > ps.PeerAddr {
			ps.LeaderAddr = pm.Metadata.Sender
			ps.Role = Follower
			log.Printf("[%s] Received Coordinator message. New leader is %s", ps.PeerAddr, ps.LeaderAddr)
		} else {
			// If the coordinator sender's priority is lower than mine, ignore the coordinator.
			log.Printf("[%s] Received Coordinator message from %s but retaining leadership due to higher priority.", ps.PeerAddr, pm.Metadata.Sender)
		}
	case Heartbeat:
        log.Printf("[%s] Received heartbeat from %s", ps.PeerAddr, pm.Metadata.Sender)
        return
    // ... handle other message types
	case GetLamport:
		// New handler: return the current lamport clock
		enc := json.NewEncoder(conn)
		resp := map[string]interface{}{
			"lamport": ps.LamportClock,
		}
		if err := enc.Encode(resp); err != nil {
			log.Printf("Error encoding lamport response: %v", err)
		}
	case GetUpdatesSince:
		log.Printf("Leader received sync request from %s for updates after %d", 
			pm.Metadata.Sender, pm.Metadata.StartTime)
		
		updates, err := ps.getUpdatesSince(pm.Metadata.StartTime)
		if err != nil {
			log.Printf("Error getting updates: %v", err)
			return
		}
		
		enc := json.NewEncoder(conn)
		for _, update := range updates {
			if err := enc.Encode(update); err != nil {
				log.Printf("Failed to send update to follower: %v", err)
				return
			}
		}
		log.Printf("Sent %d updates to %s", len(updates), pm.Metadata.Sender)
	
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
		ps.LamportClock++
		pm.Timestamp = ps.LamportClock
		log.Printf("DEBUG: Leader storing object, new timestamp %d", pm.Timestamp)
		ps.EnqueueMessage(pm)
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
			ps.LamportClock = max(ps.LamportClock, pm.Timestamp)
			log.Printf("DEBUG: Follower applying store from leader; updated clock to %d", ps.LamportClock)
			ps.EnqueueMessage(pm)

			 // Send ACK response to leader
			 ack := AckMessage{
                Status:    "OK",
                Timestamp: pm.Timestamp,
            }
            if err := enc.Encode(ack); err != nil {
                log.Printf("Error encoding ACK in StoreObject (follower applying update): %v", err)
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
		ps.LamportClock++
		pm.Timestamp = ps.LamportClock
		log.Printf("DEBUG: Leader updating object, new timestamp %d", pm.Timestamp)
		ps.EnqueueMessage(pm)
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
			ps.LamportClock = max(ps.LamportClock, pm.Timestamp)
			log.Printf("DEBUG: Follower updating object from leader; updated clock to %d", ps.LamportClock)
			ps.EnqueueMessage(pm)
			 // Send ACK response to leader
			 ack := AckMessage{
                Status:    "OK",
                Timestamp: pm.Timestamp,
            }
            if err := enc.Encode(ack); err != nil {
                log.Printf("Error encoding ACK in StoreObject (follower applying update): %v", err)
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
		ps.LamportClock++
		pm.Timestamp = ps.LamportClock
		log.Printf("DEBUG: Leader deleting object, new timestamp %d", pm.Timestamp)
		ps.EnqueueMessage(pm)
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
			ps.LamportClock = max(ps.LamportClock, pm.Timestamp)
			log.Printf("DEBUG: Follower deleting object from leader; updated clock to %d", ps.LamportClock)
			ps.EnqueueMessage(pm)
			 // Send ACK response to leader
			 ack := AckMessage{
                Status:    "OK",
                Timestamp: pm.Timestamp,
            }
            if err := enc.Encode(ack); err != nil {
                log.Printf("Error encoding ACK in StoreObject (follower applying update): %v", err)
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
    var ack AckMessage
    if err := dec.Decode(&ack); err != nil {
        return fmt.Errorf("failed to decode ack: %v", err)
    }
    
    if ack.Status != "OK" || ack.Timestamp != pm.Timestamp {
        return fmt.Errorf("follower failed to ack message properly")
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

// StoreObjects writes an array of objects with quorum consistency.
// If the node is a follower, it forwards writes to the leader.
// If the node is the leader, it ensures writes are replicated to a majority of nodes before acknowledging.
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
        // Calculate the quorum size (majority of nodes including leader)
        quorumSize := (len(ps.knownPeers)) / 2
        
        for _, obj := range objs {
            ps.LamportClock++
            currentTs := ps.LamportClock
            pm := PeerMessage{Type: StoreObject, Data: obj, Timestamp: currentTs}
            log.Printf("DEBUG: Leader storing batch object, timestamp %d", currentTs)
            
            // Create a channel to collect acknowledgments
            ackChan := make(chan bool, len(ps.knownPeers))
            var wg sync.WaitGroup
            
            // Enqueue the message locally (counts as 1 acknowledgment)
            ps.EnqueueMessage(pm)
            ackCount := 1 // Start with 1 for the leader itself
            
            // Push update to followers in parallel
            for _, addr := range ps.knownPeers {
                wg.Add(1)
                go func(peerAddr string) {
                    defer wg.Done()
                    if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
                        log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
                        ackChan <- false
                    } else {
                        ackChan <- true
                    }
                }(addr)
            }
            
            // Wait for acknowledgments in a separate goroutine
            go func() {
                wg.Wait()
                close(ackChan)
            }()
            
            // Count acknowledgments until we reach quorum or timeout
            timeout := time.After(5 * time.Second)
        ackLoop:
            for {
                select {
                case ack, ok := <-ackChan:
                    if !ok {
                        break ackLoop // all followers responded
                    }
                    if ack {
                        ackCount++
                        if ackCount >= quorumSize {
                            break ackLoop // quorum reached
                        }
                    }
                case <-timeout:
                    log.Printf("Timeout waiting for quorum acknowledgments")
                    break ackLoop
                }
            }
            
            if ackCount < quorumSize {
                return nil, fmt.Errorf("failed to achieve quorum (%d/%d nodes responded)", ackCount, quorumSize)
            }
            
            log.Printf("DEBUG: Achieved quorum (%d/%d) for write at timestamp %d", ackCount, quorumSize, currentTs)
        }
        
        return map[string]string{"status": "OK"}, nil
    }
    return nil, fmt.Errorf("invalid role")
}

// UpdateObject updates an object with quorum consistency
func (ps *PeerServer) UpdateObject(obj database.StoredObject) (map[string]string, error) {
    pm := PeerMessage{Type: UpdateObject, Data: obj}
    if ps.Role == Follower {
        return ps.ForwardRequestToLeader(pm)
    } else if ps.Role == Leader {
        ps.LamportClock++
        currentTs := ps.LamportClock
        pm := PeerMessage{Type: UpdateObject, Data: obj, Timestamp: currentTs}
        log.Printf("DEBUG: Leader updating object, timestamp %d", currentTs)
        
        // Calculate quorum size (majority of nodes including leader)
        quorumSize := (len(ps.knownPeers)) / 2
        
        // Enqueue locally (counts as 1 ack)
        ps.EnqueueMessage(pm)
        ackCount := 1
        
        // Create channel for acknowledgments
        ackChan := make(chan bool, len(ps.knownPeers))
        var wg sync.WaitGroup
        
        // Push to followers in parallel
        for _, addr := range ps.knownPeers {
            wg.Add(1)
            go func(peerAddr string) {
                defer wg.Done()
                if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
                    log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
                    ackChan <- false
                } else {
                    ackChan <- true
                }
            }(addr)
        }
        
        // Wait for acks in separate goroutine
        go func() {
            wg.Wait()
            close(ackChan)
        }()
        
        // Count acknowledgments
        timeout := time.After(5 * time.Second)
    ackLoop:
        for {
            select {
            case ack, ok := <-ackChan:
                if !ok {
                    break ackLoop // all responses received
                }
                if ack {
                    ackCount++
                    if ackCount >= quorumSize {
                        break ackLoop // quorum achieved
                    }
                }
            case <-timeout:
                log.Printf("Timeout waiting for quorum acknowledgments")
                break ackLoop
            }
        }
        
        if ackCount < quorumSize {
            return nil, fmt.Errorf("failed to achieve quorum (%d/%d nodes responded)", ackCount, quorumSize)
        }
        
        log.Printf("DEBUG: Achieved quorum (%d/%d) for update at timestamp %d", ackCount, quorumSize, currentTs)
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
        pm := PeerMessage{
            Type: DeleteObject, 
            Data: database.StoredObject{UserId: userId, UserMessageID: userMessageId},
            Timestamp: currentTs,
        }
        log.Printf("DEBUG: Leader deleting object, timestamp %d", currentTs)
        
        // Calculate quorum size (majority of nodes including leader)
        quorumSize := (len(ps.knownPeers)) / 2
        
        // Enqueue locally (counts as 1 ack)
        ps.EnqueueMessage(pm)
        ackCount := 1
        
        // Create channel for acknowledgments
        ackChan := make(chan bool, len(ps.knownPeers))
        var wg sync.WaitGroup
        
        // Push to followers in parallel
        for _, addr := range ps.knownPeers {
            wg.Add(1)
            go func(peerAddr string) {
                defer wg.Done()
                if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
                    log.Printf("Leader failed to push delete to follower %s: %v", peerAddr, err)
                    ackChan <- false
                } else {
                    ackChan <- true
                }
            }(addr)
        }
        
        // Wait for acks in separate goroutine
        go func() {
            wg.Wait()
            close(ackChan)
        }()
        
        // Count acknowledgments
        timeout := time.After(5 * time.Second)
    ackLoop:
        for {
            select {
            case ack, ok := <-ackChan:
                if !ok {
                    break ackLoop // all responses received
                }
                if ack {
                    ackCount++
                    if ackCount >= quorumSize {
                        break ackLoop // quorum achieved
                    }
                }
            case <-timeout:
                log.Printf("Timeout waiting for quorum acknowledgments")
                break ackLoop
            }
        }
        
        if ackCount < quorumSize {
            return nil, fmt.Errorf("failed to achieve quorum (%d/%d nodes responded)", ackCount, quorumSize)
        }
        
        log.Printf("DEBUG: Achieved quorum (%d/%d) for delete at timestamp %d", ackCount, quorumSize, currentTs)
        return map[string]string{"status": "OK"}, nil
    }
    return nil, fmt.Errorf("invalid role")
}

// SynchronizeWithLeader ensures the follower is up-to-date with the leader's state
func (ps *PeerServer) SynchronizeWithLeader() error {
    if ps.Role != Follower || ps.LeaderAddr == "" {
        return nil
    }

    // Create a new connection for the synchronization session
    conn, err := net.DialTimeout("tcp", ps.LeaderAddr, 5*time.Second)
    if err != nil {
        return fmt.Errorf("failed to connect to leader: %v", err)
    }
    defer conn.Close()

    // Set up encoder/decoder with buffer control
    enc := json.NewEncoder(conn)
    dec := json.NewDecoder(conn)
    
    // 1. First get the leader's current state
    leaderState := struct {
        Lamport    int64 `json:"lamport"`
        LastUpdate int64 `json:"last_update"`
    }{}

    if err := enc.Encode(PeerMessage{Type: GetLamport}); err != nil {
        return fmt.Errorf("failed to send GetLamport request: %v", err)
    }

    if err := dec.Decode(&leaderState); err != nil {
        return fmt.Errorf("failed to decode leader state: %v", err)
    }

    // 2. Check if we're already synchronized
    if ps.LamportClock >= leaderState.Lamport {
        log.Printf("DEBUG: Already synchronized (local:%d leader:%d)", 
            ps.LamportClock, leaderState.Lamport)
        return nil
    }

    log.Printf("DEBUG: Starting synchronization (local:%d leader:%d)", 
        ps.LamportClock, leaderState.Lamport)

    // 3. Request missing updates starting from our last timestamp + 1
    syncReq := PeerMessage{
        Type: GetUpdatesSince,
        Metadata: InternalData{
            Sender:    ps.PeerAddr,
            StartTime: ps.LamportClock + 1, // Don't re-send what we already have
        },
    }

    if err := enc.Encode(syncReq); err != nil {
        return fmt.Errorf("failed to send sync request: %v", err)
    }

    // 4. Process incoming updates with progress tracking
    var (
        updateCount    int
        lastTimestamp int64
        batchStart    = time.Now()
    )

    for {
        var update PeerMessage
        if err := dec.Decode(&update); err != nil {
            if err == io.EOF {
                break // Normal termination
            }
            return fmt.Errorf("failed to decode update: %v", err)
        }

        // Validate update sequence
        if update.Timestamp <= ps.LamportClock {
            log.Printf("DEBUG: Skipping duplicate update (ts:%d)", update.Timestamp)
            continue
        }

        if update.Timestamp <= lastTimestamp {
            log.Printf("WARN: Out-of-order update (current:%d received:%d)", 
                lastTimestamp, update.Timestamp)
            continue
        }

        // Apply the update
        ps.LamportClock = update.Timestamp
        lastTimestamp = update.Timestamp
        ps.EnqueueMessage(update)
        updateCount++

        // Log progress every 100 updates or 1 second
        if updateCount%100 == 0 || time.Since(batchStart) > time.Second {
            log.Printf("DEBUG: Sync progress - %d updates, current ts:%d", 
                updateCount, update.Timestamp)
            batchStart = time.Now()
        }

        // Check if we've caught up
        if update.Timestamp >= leaderState.Lamport {
            break
        }
    }

    // 5. Final synchronization check
    if ps.LamportClock < leaderState.Lamport {
        return fmt.Errorf("partial synchronization (local:%d leader:%d)", 
            ps.LamportClock, leaderState.Lamport)
    }

    log.Printf("DEBUG: Synchronization complete - %d updates applied, new clock: %d", 
        updateCount, ps.LamportClock)
    return nil
}

//maintain the update log
func (ps *PeerServer) appendToUpdateLog(pm PeerMessage) {
    ps.updateLogMutex.Lock()
    defer ps.updateLogMutex.Unlock()
    
    ps.updateLog = append(ps.updateLog, pm)
    
    // Prune if needed
    if len(ps.updateLog) > ps.maxLogSize {
        ps.updateLog = ps.updateLog[len(ps.updateLog)-ps.maxLogSize:]
    }
}

func (ps *PeerServer) getUpdatesSince(since int64) ([]PeerMessage, error) {
    ps.updateLogMutex.Lock()
    defer ps.updateLogMutex.Unlock()
    
    var updates []PeerMessage
    
    // Binary search to find first relevant update
    left := 0
    right := len(ps.updateLog) - 1
    startIdx := -1
    
    for left <= right {
        mid := left + (right-left)/2
        if ps.updateLog[mid].Timestamp > since {
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
	defer conn.Close()

	// Send a heartbeat message.
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
	// Optionally, you could wait for an acknowledgment here.

	return true
}


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

func (ps *PeerServer) StartElection() {
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
    
    // Filter out unreachable higher-priority peers.
    var liveHigherPeers []string
    var updatedKnownPeers []string
    for _, addr := range ps.knownPeers {
        if addr > ps.PeerAddr { // using lexicographical order as priority
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
    // Optionally update knownPeers to remove unreachable nodes.
    ps.knownPeers = updatedKnownPeers
    
    log.Printf("[%s] Found %d reachable higher priority peer(s): %v", ps.PeerAddr, len(liveHigherPeers), liveHigherPeers)
    
    if len(liveHigherPeers) == 0 {
        ps.becomeLeader()
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
    
    // Increase timeout to allow responses.
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
        ps.becomeLeader()
    }
}


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
    ps.LeaderAddr = ps.PeerAddr

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

// Helper function to get a peer's latest timestamp
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

// Helper function to synchronize with a specific peer
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

// Helper function to announce leadership to a peer
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



func (ps *PeerServer) EnqueueMessage(pm PeerMessage) {
	ps.queueLock.Lock()
	defer ps.queueLock.Unlock()
	heap.Push(&ps.msgQueue, &MessageItem{message: pm})
	log.Printf("DEBUG: Enqueued message: type %d, timestamp %d. Queue length now: %d", pm.Type, pm.Timestamp, ps.msgQueue.Len())

}

// processQueue continuously processes messages from the queue in order and updates log
func (ps *PeerServer) processQueue() {
    var expectedTimestamp int64 = 1
    for {
        ps.queueLock.Lock()
        if ps.msgQueue.Len() > 0 {
            item := ps.msgQueue[0]
            if item.message.Timestamp <= expectedTimestamp {
                heap.Pop(&ps.msgQueue)
                ps.queueLock.Unlock()

                // Apply the message if it's exactly what we expect
                if item.message.Timestamp == expectedTimestamp {
                    ps.applyMessage(item.message)
                    ps.appendToUpdateLog(item.message) // Log applied messages
                    expectedTimestamp++
                } else {
					// If it's older (shouldn't happen with proper Lamport clocks), apply but don't increment
                    ps.applyMessage(item.message)
                    ps.appendToUpdateLog(item.message)
                }
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

