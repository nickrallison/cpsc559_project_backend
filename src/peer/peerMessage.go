package peer

import "cpsc559/src/database"

// PeerMessageType defines the types of messages between peers.
type PeerMessageType int

const (
	GetObject PeerMessageType = iota
	GetUpdatesSince PeerMessageType = iota
	StoreObject
	UpdateObject
	DeleteObject
	AddPeer
	ConfirmPeer
	// New election-related message types:
	Election
	ElectionAnswer
	Coordinator
	Heartbeat
	GetLamport
	// Message types for quorum consistency
	// For store operations:
	PrepareStoreObject = 100
    CommitStoreObject  = 101
    AbortStoreObject   = 102

	// For update operations:
    PrepareUpdateObject PeerMessageType = 110
    CommitUpdateObject  PeerMessageType = 111
    AbortUpdateObject   PeerMessageType = 112

    // For delete operations:
    PrepareDeleteObject PeerMessageType = 120
    CommitDeleteObject  PeerMessageType = 121
    AbortDeleteObject   PeerMessageType = 122
)

// InternalData holds extra metadata.
type InternalData struct {
	Sender    	string
	PeerToAdd 	string
	StartTime 	int64  // for synchronization requests
}

// PeerMessage represents a message between peers.
type PeerMessage struct {
	Type     PeerMessageType
	Data     database.StoredObject
	Timestamp int64 
	Metadata InternalData
}

// AckMessage represents an acknowledgment response
type AckMessage struct {
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}
