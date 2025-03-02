package peer

import (
	"cpsc559/src/database"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
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
}

// NewPeerServer creates and returns a new instance.
func NewPeerServer(role Role, port, leaderAddr, peers string, db *sql.DB) PeerServer {
	ps := PeerServer{
		Role:       role,
		PeerAddr:   "localhost:" + port,
		LeaderAddr: leaderAddr,
		DB:         db,
	}
	if role == Leader && peers != "" {
		ps.knownPeers = strings.Split(peers, ",")
	}
	return ps
}

// Start begins listening for incoming peer messages.
func (ps *PeerServer) Start() {
	ln, err := net.Listen("tcp", ps.PeerAddr)
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
	default:
		log.Printf("Unhandled peer message type: %d", pm.Type)
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
		if _, err := database.InsertObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
			log.Printf("Leader failed to store object: %v", err)
		}
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
			// This update is coming from the leader; apply the update locally.
			if _, err := database.InsertObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
				log.Printf("Follower failed to store object (from leader): %v", err)
				enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
				return
			}
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
		if _, err := database.UpdateObjects(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
			log.Printf("Leader failed to update object: %v", err)
		}
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
			if _, err := database.UpdateObjects(ps.DB, pm.Data.UserId, pm.Data.UserMessageID, pm.Data.Data); err != nil {
				log.Printf("Follower failed to update object (from leader): %v", err)
				enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
				return
			}
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
		if _, err := database.DeleteObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID); err != nil {
			log.Printf("Leader failed to delete object: %v", err)
		}
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
			if _, err := database.DeleteObject(ps.DB, pm.Data.UserId, pm.Data.UserMessageID); err != nil {
				log.Printf("Follower failed to delete object (from leader): %v", err)
				enc.Encode(map[string]string{"status": "ERROR", "message": err.Error()})
				return
			}
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
			if _, err := database.InsertObject(ps.DB, obj.UserId, obj.UserMessageID, obj.Data); err != nil {
				return nil, err
			}
			for _, addr := range ps.knownPeers {
				go func(peerAddr string, o database.StoredObject) {
					pm := PeerMessage{Type: StoreObject, Data: o}
					if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
						log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
					}
				}(addr, obj)
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
		if _, err := database.UpdateObjects(ps.DB, obj.UserId, obj.UserMessageID, obj.Data); err != nil {
			return nil, err
		}
		for _, addr := range ps.knownPeers {
			go func(peerAddr string, o database.StoredObject) {
				pm := PeerMessage{Type: UpdateObject, Data: o}
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Leader failed to push update to follower %s: %v", peerAddr, err)
				}
			}(addr, obj)
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
		if _, err := database.DeleteObject(ps.DB, userId, userMessageId); err != nil {
			return nil, err
		}
		for _, addr := range ps.knownPeers {
			go func(peerAddr string, uid, umid int) {
				pm := PeerMessage{Type: DeleteObject, Data: database.StoredObject{UserId: uid, UserMessageID: umid}}
				if err := ps.pushUpdateToPeer(peerAddr, pm); err != nil {
					log.Printf("Leader failed to push delete to follower %s: %v", peerAddr, err)
				}
			}(addr, userId, userMessageId)
		}
		return map[string]string{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}
