package raft

import (
	"fmt"
	"sync"

	"github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
)

// FSM is the finite-state-machine of the raft log
type FSM struct {
	NodeID uint32
	Nodes  map[uint32]types.Node
	Topics map[string]types.Topic
	sync.RWMutex
}

// StoreNode stores a node (broker) in the FSM
func (fsm *FSM) StoreNode(node types.Node) {
	fsm.Lock()
	defer fsm.Unlock()
	fsm.Nodes[node.NodeID] = node
}

// StoreTopic stores a topic in the FSM
func (fsm *FSM) StoreTopic(topic types.Topic) {
	fsm.Lock()
	defer fsm.Unlock()
	if _, ok := fsm.Topics[topic.Name]; !ok {
		fsm.Topics[topic.Name] = types.Topic{Name: topic.Name, Partitions: make(map[uint32]types.PartitionState), Configs: topic.Configs}
	}
}

// StorePartition stores a partition in the FSM
func (fsm *FSM) StorePartition(partition types.PartitionState) error {
	fsm.Lock()
	defer fsm.Unlock()
	if _, ok := fsm.Topics[partition.Topic]; !ok {
		return fmt.Errorf("topic %v doesn't exist in raft FSM", partition.Topic)
	}
	fsm.Topics[partition.Topic].Partitions[partition.PartitionIndex] = partition

	logging.Info("StorePartition partition.LeaderID %v, fsm.NodeID %v", partition.LeaderID, fsm.NodeID)
	if partition.LeaderID == fsm.NodeID { //|| slices.Contains(partition.ReplicaNodes,  fsm.NodeID)
		return storage.EnsurePartition(partition.Topic, partition.PartitionIndex)
	}
	return nil
}

// GetNode retrieves a node (broker) from the FSM
func (fsm *FSM) GetNode(nodeID uint32) (types.Node, bool) {
	fsm.RLock()
	defer fsm.RUnlock()
	node, exists := fsm.Nodes[nodeID]
	return node, exists
}

// GetTopic retrieves a topic from the FSM
func (fsm *FSM) GetTopic(topicName string) (types.Topic, bool) {
	fsm.RLock()
	defer fsm.RUnlock()
	topic, exists := fsm.Topics[topicName]
	return topic, exists
}

// GetPartition retrieves a partition from the FSM
func (fsm *FSM) GetPartition(topicName string, partitionIndex uint32) (types.PartitionState, bool) {
	fsm.RLock()
	defer fsm.RUnlock()
	topic, topicExists := fsm.Topics[topicName]
	if !topicExists {
		return types.PartitionState{}, false
	}
	partition, partitionExists := topic.Partitions[partitionIndex]
	return partition, partitionExists
}

// TopicExists checks if topicName exists in the FSM
func (fsm *FSM) TopicExists(topicName string) bool {
	fsm.RLock()
	defer fsm.RUnlock()
	_, exists := fsm.Topics[topicName]
	return exists
}
