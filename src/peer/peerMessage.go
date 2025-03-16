package peer

import "cpsc559/src/database"

// PeerMessageType defines the types of messages between peers.
type PeerMessageType int

const (
	GetObject PeerMessageType = iota
	StoreObject
	UpdateObject
	DeleteObject
	AddPeer
	ConfirmPeer
)

// InternalData holds extra metadata.
type InternalData struct {
	Sender    string
	PeerToAdd string
}

// PeerMessage represents a message between peers.
type PeerMessage struct {
	Type     PeerMessageType
	Data     database.StoredObject
	Timestamp int64 
	Metadata InternalData
}
