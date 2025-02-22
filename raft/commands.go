package raft

import (
	"encoding/json"
	"fmt"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/types"
)

// CommandType is a raft log command type
type CommandType int

// Commands types that can be applied to the raft log to change the state machine
const (
	AddNode CommandType = iota
	RemoveNode
	AddTopic
	RemoveTopic
	AddPartition
	RemovePartition
)

// Command represents a command type with its payload
type Command struct {
	Kind    CommandType
	Payload json.RawMessage
}

// ApplyCommand to append a command to the raft log
func (kf *FSM) ApplyCommand(cmd Command) error {
	log.Info("Inside ApplyCommand %v", cmd.Kind)
	switch cmd.Kind {
	case AddNode:
	case RemoveNode:
	case AddTopic:
		var topic types.Topic
		err := json.Unmarshal(cmd.Payload, &topic)
		if err != nil {
			return fmt.Errorf("could not parse topic: %s", err)
		}
		log.Debug("Raft ApplyCommand AddTopic: %+v", topic)
		// kf.DB.Store(sp.Key, sp.Value)
		kf.StoreTopic(topic)

	case AddPartition:
		var partition types.PartitionState
		err := json.Unmarshal(cmd.Payload, &partition)
		if err != nil {
			return fmt.Errorf("could not parse partition command: %s", err)
		}
		log.Debug("Raft ApplyCommand AddPartition: %+v", partition)
		err = kf.StorePartition(partition)
		if err != nil {
			return fmt.Errorf("error applying partition %+v command: %s", partition, err)
		}
	default:
		return fmt.Errorf("unknown command type: %#v", cmd.Kind)
	}
	return nil
}

// EncodeLogEntry converts a raft log entry into bytes
// TODO: use protobuf or some better encoding
func EncodeLogEntry(entryType CommandType, entry any) (res []byte, err error) {
	cmd := Command{Kind: entryType}
	cmd.Payload, err = json.Marshal(entry)
	if err != nil {
		return
	}
	res, err = json.Marshal(cmd)
	return
}
