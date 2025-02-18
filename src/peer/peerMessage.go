package peer

import "cpsc559/src/database"

type PeerMessageType int

const (
	GetObjectPeerMessageType = iota
	StoreObject
	UpdateObject
	DeleteObject
)

type PeerMessage struct {
	Type PeerMessageType
	Data database.StoredObject
}
