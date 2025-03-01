package protocol

import (
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/types"
)

// FindCoordinatorRequestV1To3 represents the request for coordinator finding
type FindCoordinatorRequestV1To3 struct {
	Key     types.NonCompactString
	KeyType uint8
}

// FindCoordinatorRequestV4Plus represents the request for coordinator finding
type FindCoordinatorRequestV4Plus struct {
	// Key     string `kafka:"CompactString"`
	KeyType         uint8
	CoordinatorKeys []string
}

// FindCoordinatorResponse represents the response for a coordinator finding request.
type FindCoordinatorResponse struct {
	ThrottleTimeMs uint32
	Coordinators   []FindCoordinatorResponseCoordinator
}

// FindCoordinatorResponseCoordinator represents  a coordinator.
type FindCoordinatorResponseCoordinator struct {
	Key          string `kafka:"CompactString"`
	NodeID       uint32
	Host         string `kafka:"CompactString"`
	Port         uint32
	ErrorCode    uint16
	ErrorMessage string `kafka:"CompactString"`
}

func (b *Broker) getFindCoordinatorResponse(req types.Request) []byte {
	log.Info("getFindCoordinatorResponse %v", req.Body)
	decoder := serde.NewDecoder(req.Body)
	var key string
	if req.RequestAPIVersion < 4 {
		// before v4, we used String instead of CompactString
		findCoordinatorRequest := decoder.Decode(&FindCoordinatorRequestV1To3{}).(*FindCoordinatorRequestV1To3)
		log.Info("FindCoordinatorRequestV1To3 %+v", findCoordinatorRequest)
		key = string(findCoordinatorRequest.Key)
	} else {
		findCoordinatorRequest := decoder.Decode(&FindCoordinatorRequestV4Plus{}).(*FindCoordinatorRequestV4Plus)
		log.Info("FindCoordinatorRequestV4Plus %+v", findCoordinatorRequest)

		if len(findCoordinatorRequest.CoordinatorKeys) > 0 {
			key = findCoordinatorRequest.CoordinatorKeys[0]
		}
	}

	response := FindCoordinatorResponse{
		Coordinators: []FindCoordinatorResponseCoordinator{{
			Key:    key,
			NodeID: b.FSM.NodeID,
			Host:   state.Config.BrokerHost,
			Port:   uint32(state.Config.BrokerPort),
		}},
	}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}
