package types

// Topic in the state
type Topic struct {
	TopicID    [16]byte
	Name       string
	Partitions map[uint32]PartitionState
	Configs    map[string]string
}

// Node represents a broker
type Node struct {
	NodeID       uint32
	Host         string
	Port         uint32
	Rack         string
	IsController bool
}

// PartitionState represents a partition in the state
type PartitionState struct {
	Topic           string // Topic Name
	PartitionIndex  uint32
	LeaderID        uint32
	LeaderEpoch     uint32
	ReplicaNodes    []uint32
	IsrNodes        []uint32
	OfflineReplicas []uint32
}
