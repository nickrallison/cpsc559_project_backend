package peer

import (
	"cpsc559/src/database"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

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
func (ps *PeerServer) StoreObjects(objs []database.StoredObject) (map[string]interface{}, error) {
	if ps.Role == Follower {
		var lastResp map[string]interface{}
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

		return map[string]interface{}{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}

// UpdateObject and DeleteObject follow a similar replication logic as StoreObjects
// ensuring the update is applied locally and replicated to a quorum of followers
func (ps *PeerServer) UpdateObject(obj database.StoredObject) (map[string]interface{}, error) {
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
		return map[string]interface{}{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
}

// DeleteObject deletes an object with quorum consistency
func (ps *PeerServer) DeleteObject(userId int, userMessageId int) (map[string]interface{}, error) {
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
		return map[string]interface{}{"status": "OK"}, nil
	}
	return nil, fmt.Errorf("invalid role")
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
