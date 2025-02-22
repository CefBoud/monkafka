package raft

import (
	"encoding/json"
	"fmt"

	"io"

	"github.com/CefBoud/monkafka/logging"
	"github.com/hashicorp/raft"
)

// Apply applies a `raft.Log` to the FSM
func (kf *FSM) Apply(log *raft.Log) any {
	logging.Info("Inside KV APPLY %v", log.Type)
	switch log.Type {
	case raft.LogCommand:
		var cmd Command
		err := json.Unmarshal(log.Data, &cmd)
		logging.Info("case raft.LogCommand Error: %v", err)
		if err != nil {
			return fmt.Errorf("could not parse payload: %s", err)
		}
		kf.ApplyCommand(cmd)
	default:
		return fmt.Errorf("unknown raft log type: %#v", log.Type)
	}

	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                          {}

// Snapshot snapshots the FSM into a struct that implements the raft.FSMSnapshot interface
func (kf *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

// Restore is used to restore an FSM from a snapshot
func (kf *FSM) Restore(rc io.ReadCloser) error {
	// deleting first isn't really necessary since there's no exposed DELETE operation anyway.
	// so any changes over time will just get naturally overwritten
	decoder := json.NewDecoder(rc)
	logging.Info("Inside RESTORE")
	for decoder.More() {
		var cmd Command
		err := decoder.Decode(&cmd)
		fmt.Printf("inside restore, parsed command %+v \n", cmd)
		if err != nil {
			return fmt.Errorf("could not decode entry during restore: %s", err)
		}
		kf.ApplyCommand(cmd)
	}

	return rc.Close()
}
