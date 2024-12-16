package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

var Encoding = binary.BigEndian

var logDir = filepath.Join("/tmp", "MonKafka")

// https://kafka.apache.org/protocol#protocol_api_keys
var ProduceKey = uint16(0)
var FetchKey = uint16(1)
var MetadataKey = uint16(3)
var OffsetFetchKey = uint16(9)
var FindCoordinatorKey = uint16(10)
var JoinGroupKey = uint16(11)
var HeartbeatKey = uint16(12)
var SyncGroupKey = uint16(14)
var APIVersionKey = uint16(18)
var CreateTopicKey = uint16(19)
var InitProducerIdKey = uint16(22)

var MINUS_ONE = -1
var State = make(map[string]uint64)

// func getBytePositionAfterRequestHeader(requestBytes []byte) uint16 {
// 	// msg len 4 + api key 2 + api version 2 + correlation id 4
// 	// + len(client_id) 2 + client_id X +  no tags 1 + next pos 1
// 	return 4 + 2 + 2 + 4 + 2 + Encoding.Uint16(requestBytes[12:]) + 1 + 1
// }

// APIVersion (Api key = 18)
func getAPIVersionResponse(correlationID uint32) []byte {
	APIVersions := APIVersionsResponse{
		errorCode: 0,
		apiKeys: []APIKey{
			{apiKey: ProduceKey, minVersion: 0, maxVersion: 11},
			{apiKey: FetchKey, minVersion: 12, maxVersion: 12},
			{apiKey: MetadataKey, minVersion: 0, maxVersion: 12},
			{apiKey: OffsetFetchKey, minVersion: 0, maxVersion: 9},

			{apiKey: FindCoordinatorKey, minVersion: 0, maxVersion: 6},
			{apiKey: JoinGroupKey, minVersion: 0, maxVersion: 9},
			{apiKey: HeartbeatKey, minVersion: 0, maxVersion: 4},
			{apiKey: SyncGroupKey, minVersion: 0, maxVersion: 5},
			{apiKey: APIVersionKey, minVersion: 0, maxVersion: 4},
			{apiKey: CreateTopicKey, minVersion: 0, maxVersion: 7},
			{apiKey: InitProducerIdKey, minVersion: 0, maxVersion: 5},
		},
	}
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.PutInt16(APIVersions.errorCode)
	encoder.PutInt32(uint32(len(APIVersions.apiKeys)))
	for _, k := range APIVersions.apiKeys {
		encoder.PutInt16(k.apiKey)
		encoder.PutInt16(k.minVersion)
		encoder.PutInt16(k.maxVersion)
	}
	encoder.PutInt32(0) //ThrottleTime
	encoder.PutLen()
	// fmt.Println("APIversion b[:offset] : ", encoder.Bytes())
	return encoder.Bytes()

}

// Metadata	(Api key = 3)
func getMetadataResponse(correlationID uint32) []byte {
	topicName := "titi" // TODO get this from request
	response := metadataResponse{
		throttle_time_ms: 0,
		brokers: []broker{
			{node_id: 1, host: "localhost", port: 9092, rack: ""},
			// {node_id: 1, host: "host.docker.internal", port: 9092, rack: ""},
		},
		cluster_id:    "ABRACADABRA",
		controller_id: 1,
		// topics:        []topic{},
		topics: []topic{
			{error_code: 0, name: topicName, topic_id: [16]byte{}, is_internal: false, partitions: []partition{
				{
					error_code:       0,
					partition_index:  0,
					leader_id:        1,
					replica_nodes:    []uint32{1},
					isr_nodes:        []uint32{1},
					offline_replicas: []uint32{}},
				// {
				// 	error_code:       0,
				// 	partition_index:  1,
				// 	leader_id:        1,
				// 	replica_nodes:    []uint32{1},
				// 	isr_nodes:        []uint32{1},
				// 	offline_replicas: []uint32{}},
			}},
		},
	}
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // header end

	encoder.PutInt32(uint32(response.throttle_time_ms))

	// brokers
	encoder.PutCompactArrayLen(len(response.brokers))
	// Encoding.PutUint32(b[offset:], uint32(len(response.brokers)))
	// offset += 4
	for _, bk := range response.brokers {
		encoder.PutInt32(uint32(bk.node_id))
		encoder.PutString(bk.host)
		encoder.PutInt32(uint32(bk.port))
		encoder.PutString(bk.rack) // compact nullable string
		encoder.EndStruct()
	}
	// cluster id compact_string
	encoder.PutString(response.cluster_id)
	encoder.PutInt32(uint32(response.controller_id))
	// topics compact_array
	encoder.PutCompactArrayLen(len(response.topics))
	for _, tp := range response.topics {
		encoder.PutInt16(uint16(tp.error_code))
		encoder.PutString(tp.name)
		encoder.PutBytes(tp.topic_id[:])
		encoder.PutBool(tp.is_internal)
		encoder.PutCompactArrayLen(len(tp.partitions))

		for _, par := range tp.partitions {
			encoder.PutInt16(uint16(par.error_code))
			encoder.PutInt32(uint32(par.partition_index))
			encoder.PutInt32(uint32(par.leader_id))
			// replicas
			encoder.PutCompactArrayLen(len(par.replica_nodes))
			for _, rn := range par.replica_nodes {
				encoder.PutInt32(uint32(rn))
			}
			// isrs
			encoder.PutCompactArrayLen(len(par.isr_nodes))
			for _, isr := range par.replica_nodes {
				encoder.PutInt32(uint32(isr))
			}
			// offline
			// isrs
			encoder.PutCompactArrayLen(len(par.offline_replicas))
			for _, off := range par.offline_replicas {
				encoder.PutInt32(uint32(off))
			}
			encoder.EndStruct()
		}

		encoder.PutInt32(tp.topic_authorized_operations)
		encoder.EndStruct() // end topic
	}
	encoder.EndStruct()
	encoder.PutLen()

	// fmt.Println(" Metadata b[:offset] : ", encoder.Bytes())
	return encoder.Bytes()
}

// CreateTopics	(Api key = 19)

func getCreateTopicResponse(correlationID uint32, requestBuffer []byte) []byte {
	// get topicName
	decoder := NewDecoder(requestBuffer)
	decoder.SkipHeader()
	_ = decoder.CompactArrayLen() //topicsLen
	// TODO loop and handle this properly
	// fmt.Println("topicsLen", topicsLen)
	topicName := decoder.String()
	response := CreateTopicsResponse{
		Topics: []Topic{{Name: topicName, TopicID: [16]byte{},
			ErrorCode:         0,
			ErrorMessage:      "",
			NumPartitions:     1,
			ReplicationFactor: 1,
			Configs:           []Config{},
		}}}
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct()                       // end header
	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms

	// topics
	encoder.PutCompactArrayLen(len(response.Topics))
	for _, tp := range response.Topics {
		encoder.PutString(tp.Name)
		encoder.PutBytes(tp.TopicID[:]) // UUID
		encoder.PutInt16(tp.ErrorCode)
		encoder.PutString(tp.ErrorMessage)
		encoder.PutInt32(tp.NumPartitions)
		encoder.PutInt16(tp.ReplicationFactor)

		encoder.PutCompactArrayLen(len(tp.Configs)) // empty config array
		encoder.EndStruct()
	}
	encoder.EndStruct()
	encoder.PutLen()

	// create a dir for the topic

	err := os.MkdirAll(filepath.Join(logDir, topicName), 0750)
	fmt.Println("Created topic within ", filepath.Join(logDir, topicName))
	if err != nil {
		fmt.Println("Error creating topic directory:", err)
	}
	// fmt.Println(" CreateTopics b[:offset] : ", encoder.Bytes())
	return encoder.Bytes()
}

// InitProducerId (Api key = 22)
func getInitProducerIdResponse(correlationID uint32, requestBuffer []byte) []byte {
	// get producer id and epoch
	// i := 4 + 2 + 2 + 4 + 2 + Encoding.Uint16(requestBuffer[12:]) // msg len 4 + api key 2 + api version 2 + correlation id 4 + client id len 2 + client_id X
	// i += 1 + 1

	decoder := NewDecoder(requestBuffer)
	decoder.SkipHeader()
	_ = decoder.String() //transactional_id
	transactionTimeoutMs := decoder.UInt32()
	producerId := decoder.UInt64()
	if int(producerId) == -1 {
		producerId = 1 // TODO: set this value properly
	}
	epochId := decoder.UInt16()
	if int16(epochId) == -1 {
		epochId = 1 // TODO: set this value properly
	}
	if true {
		fmt.Println("getInitProducerIdResponse request byte: ", requestBuffer, transactionTimeoutMs, int64(producerId), int16(epochId))
	}
	response := InitProducerId{
		producer_id:    producerId,
		producer_epoch: epochId,
	}
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(response.throttle_time_ms)
	encoder.PutInt16(response.error_code)
	encoder.PutInt64(response.producer_id)
	encoder.PutInt16(response.producer_epoch)
	encoder.EndStruct()
	encoder.PutLen()
	// fmt.Println(" getInitProducerIdResponse bytes : ", encoder.Bytes())
	return encoder.Bytes()
}

// Producer (Api key = 0)
func ReadTopicData(producerRequest []byte) []TopicData {
	var topic_data []TopicData

	decoder := NewDecoder(producerRequest)
	decoder.SkipHeader()
	decoder.offset += 1 + 2 + 4 // no transactional_id + acks +timeout_ms
	nbTopics := decoder.CompactArrayLen()
	for i := 0; i < int(nbTopics); i++ {
		topicName := decoder.String()
		nbPartitions := decoder.CompactArrayLen()
		var partition_data []PartitionData
		for j := 0; j < int(nbPartitions); j++ {
			index := decoder.UInt32()
			data := decoder.BytesWithLen()
			// fmt.Println("partition_data  :", data)
			partition_data = append(partition_data, PartitionData{index: index, recordsData: data})
			decoder.EndStruct()
		}
		topic_data = append(topic_data, TopicData{name: topicName, partition_data: partition_data})
	}
	return topic_data
}

func writeProducedRecords(topic_data []TopicData) error {
	// TODO write as Record batch and fix this shit
	for _, td := range topic_data {
		for _, pd := range td.partition_data {
			partitionDir := getPartitionDir(td.name, pd.index)
			err := os.MkdirAll(partitionDir, 0750)
			fmt.Println("Writing within partition dir ", partitionDir)
			if err != nil {
				fmt.Println("Error creating topic directory:", err)
				return err
			}
			Encoding.PutUint64(pd.recordsData[1:], State[td.name+string(pd.index)])
			State[td.name+string(pd.index)]++
			// file.Write(pd.recordsData)
			err = AppendRecord(td.name, pd.index, pd.recordsData)
			if err != nil {
				fmt.Println("Error AppendRecord:", err)
				return err
			}
			fmt.Println("produce recordsByte", pd.recordsData)
		}

	}
	return nil
}
func getProduceResponse(correlationID uint32, producerRequest []byte) []byte {
	topic_data := ReadTopicData(producerRequest)
	err := writeProducedRecords(topic_data)
	if err != nil {
		fmt.Println("Error opening partition file:", err)
		os.Exit(1)
	}
	// TODO => write records to disk. These are only records, they need to be formatted as a RecordBatch
	fmt.Println("topic_data", topic_data)
	response := ProduceResponse{}

	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // end header

	for _, td := range topic_data {
		produceTopicResponse := ProduceTopicResponse{Name: td.name}
		// response.ProduceTopicResponses = append(response.ProduceTopicResponses, ProduceTopicResponse{Name: td.name})
		for _, pd := range td.partition_data {
			produceTopicResponse.ProducePartitionResponses = append(produceTopicResponse.ProducePartitionResponses, ProducePartitionResponse{Index: pd.index, LogAppendTimeMs: NowAsUnixMilli(),

				BaseOffset: 0}) //State[td.name+string(pd.index)]
		}
		response.ProduceTopicResponses = append(response.ProduceTopicResponses, produceTopicResponse)
	}

	encoder.PutCompactArrayLen(len(response.ProduceTopicResponses))
	for _, topicResp := range response.ProduceTopicResponses {
		encoder.PutString(topicResp.Name)
		encoder.PutCompactArrayLen(len(topicResp.ProducePartitionResponses))
		for _, partitionResp := range topicResp.ProducePartitionResponses {
			encoder.PutInt32(partitionResp.Index)
			encoder.PutInt16(partitionResp.ErrorCode)
			encoder.PutInt64(partitionResp.BaseOffset)
			encoder.PutInt64(partitionResp.LogAppendTimeMs)
			encoder.PutInt64(partitionResp.LogStartOffset)
			// no erros for now
			encoder.PutCompactArrayLen(len(partitionResp.RecordErrors))
			// TODO add record errros

			encoder.PutString(partitionResp.ErrorMessage)
			encoder.EndStruct()
		}
		encoder.EndStruct()
	}

	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms
	encoder.EndStruct()
	encoder.PutLen()
	fmt.Println(" getProduceResponse bytes : ", encoder.Bytes())
	return encoder.Bytes()
}

func getFindCoordinatorResponse(correlationID uint32, request []byte) []byte {
	fmt.Println("getFindCoordinatorResponse request byte: ", request)
	// coord_key := "console-consumer-22229"
	// TODO: get requested coordinator keys
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(0) // throttle_time_ms
	// {node_id: 1, host: "localhost", port: 9092, rack: ""},
	coordinators := []Coordinator{{
		Key:    "dummy", //"console-consumer-22229",
		NodeID: 1,
		Host:   "localhost",
		Port:   9092,
	}}
	encoder.PutCompactArrayLen(len(coordinators))
	for _, c := range coordinators {
		encoder.PutString(c.Key)
		encoder.PutInt32(c.NodeID)
		encoder.PutString(c.Host)
		encoder.PutInt32(c.Port)
		encoder.PutInt16(c.ErrorCode)
		encoder.PutString(c.ErrorMessage)
		encoder.EndStruct()
	}
	encoder.EndStruct()
	encoder.PutLen()
	fmt.Println(" getFindCoordinatorResponse bytes : ", encoder.Bytes())
	return encoder.Bytes()
}

func getJoinGroupResponse(correlationID uint32, request []byte) []byte {
	decoder := NewDecoder(request)
	decoder.SkipHeader()
	groupId := decoder.String()
	decoder.offset += 4 + 4 //session_timeout_ms + rebalance_timeout_ms
	memberId := decoder.String()
	groupInstanceId := decoder.String()
	protocolType := decoder.String()
	var protocolName []string
	var metadataBytes [][]byte
	for i := 0; i < int(decoder.CompactArrayLen()); i++ {
		protocolName = append(protocolName, decoder.String())
		metadataBytes = append(metadataBytes, decoder.Bytes())
	}

	fmt.Printf("getJoinGroupResponse: groupId: %v, groupInstanceId:%v, protcolType: %v, protocolName:%v, metadataBytes: %v", groupId, groupInstanceId, protocolType, protocolName, metadataBytes)

	response := JoinGroupResponse{
		GenerationID:   1,
		ProtocolType:   "consumer",
		ProtocolName:   "range",
		Leader:         memberId,
		SkipAssignment: false, // KIP-814 static membership (when false, the consumer group leader will send assignment)
		MemberID:       memberId,
		Members: []Member{
			{MemberID: memberId,
				Metadata: metadataBytes[0]},
		},
	}
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // end header

	encoder.PutInt32(response.ThrottleTimeMS) // throttle_time_ms
	encoder.PutInt16(response.ErrorCode)
	encoder.PutInt32(response.GenerationID)
	encoder.PutString(response.ProtocolType)
	encoder.PutString(response.ProtocolName)
	encoder.PutString(response.Leader)
	encoder.PutBool(response.SkipAssignment)
	encoder.PutString(response.MemberID)
	encoder.PutCompactArrayLen(len(response.Members))
	for _, m := range response.Members {
		encoder.PutString(m.MemberID)
		// encoder.PutCompactArrayLen(-1)
		encoder.PutString(m.GroupInstanceID)
		encoder.PutCompactArrayLen(len(m.Metadata))
		encoder.PutBytes(m.Metadata)
		encoder.EndStruct()
	}
	encoder.EndStruct()
	encoder.PutLen()
	fmt.Println(" getJoinGroupResponse bytes : ", encoder.Bytes())
	return encoder.Bytes()
}

func getHeartbeatResponse(correlationID uint32, request []byte) []byte {
	fmt.Println("heartbeat request", request)
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // end header

	encoder.PutInt32(0) //throttle_time_ms
	encoder.PutInt16(0) //error_code
	encoder.EndStruct()
	encoder.PutLen()
	fmt.Println(" getHeartbeatResponse bytes : ", encoder.Bytes())
	return encoder.Bytes()
}

func getSyncGroupResponse(correlationID uint32, request []byte) []byte {
	// get assignment bytes from request
	decoder := NewDecoder(request)
	decoder.SkipHeader()
	_ = decoder.String() //groupId
	_ = decoder.UInt32() //generationId
	_ = decoder.String() //memberId
	_ = decoder.String() //groupInstanceId
	protocolType := decoder.String()
	protocolName := decoder.String()
	nbAssignments := decoder.CompactArrayLen()
	assignmentBytes := make([][]byte, nbAssignments) // TODO : handle this properly
	for i := 0; i < int(nbAssignments); i++ {
		_ = decoder.String() //memberId
		assignmentBytes[i] = decoder.Bytes()
		decoder.EndStruct()
	}
	// fmt.Println("assignmentBytes", assignmentBytes)
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // end header

	// protocolType := "consumer" // protocolName := "range" // fmt.Println("protocolType", protocolType, protocolName)
	encoder.PutInt32(0) //throttle_time_ms
	encoder.PutInt16(0) //error_code

	encoder.PutString(protocolType)
	encoder.PutString(protocolName)

	// assignments COMPACT_BYTES
	//  SyncGroupResponseData(throttleTimeMs=0, errorCode=0, protocolType='consumer', protocolName='range', assignment=[0, 3, 0, 0, 0, 1, 0, 4, 116, 105, 116, 105, 0, 0, 0, 1, 0, 0, 0, 0, -1, -1, -1, -1])
	encoder.PutCompactArrayLen(len(assignmentBytes[0]))
	encoder.PutBytes(assignmentBytes[0])
	// end assignments
	encoder.EndStruct()
	encoder.PutLen()
	fmt.Println("getSyncGroupResponse bytes : ", encoder.Bytes())
	return encoder.Bytes()
}
func getOffsetFetchResponse(correlationID uint32, request []byte) []byte {
	decoder := NewDecoder(request)
	decoder.SkipHeader()
	_ = decoder.CompactArrayLen() // nbGroups
	groupId := decoder.String()   //groupId
	_ = decoder.String()          //memberId
	_ = decoder.UInt32()          //memberEpoch
	nbTopics := decoder.CompactArrayLen()
	topic_partitions := make(map[string][]uint32)
	for i := uint64(0); i < nbTopics; i++ {
		topicName := decoder.String()
		topic_partitions[topicName] = make([]uint32, 0)
		nbPartitions := decoder.CompactArrayLen()
		for j := uint64(0); j < nbPartitions; j++ {
			topic_partitions[topicName] = append(topic_partitions[topicName], decoder.UInt32())
		}
		decoder.EndStruct()
	}
	fmt.Println("topic_partitions", topic_partitions)

	response := OffsetFetchResponse{Groups: []OffsetFetchGroup{
		{GroupID: groupId, Topics: []OffsetFetchTopic{}},
	}}
	for tp, partitions := range topic_partitions {
		offsetFetchTopic := OffsetFetchTopic{Name: tp}
		for _, p := range partitions {
			offsetFetchTopic.Partitions = append(offsetFetchTopic.Partitions, OffsetFetchPartition{PartitionIndex: p})
		}
		response.Groups[0].Topics = append(response.Groups[0].Topics, offsetFetchTopic)
	}

	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct()                       // end header
	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms
	encoder.PutCompactArrayLen(len(response.Groups))
	for _, g := range response.Groups {
		encoder.PutString(g.GroupID)
		encoder.PutCompactArrayLen(len(g.Topics))
		for _, t := range g.Topics {
			encoder.PutString(t.Name)
			encoder.PutCompactArrayLen(len(t.Partitions))
			for _, p := range t.Partitions {
				encoder.PutInt32(p.PartitionIndex)
				encoder.PutInt64(p.CommittedOffset)
				encoder.PutInt32(p.CommittedLeaderEpoch)
				encoder.PutString(p.Metadata)
				encoder.PutInt16(p.ErrorCode)
				encoder.EndStruct()
			}
			encoder.PutInt16(t.ErrorCode)
			encoder.EndStruct()
		}
		encoder.EndStruct()
	}
	encoder.EndStruct()
	encoder.PutLen()
	fmt.Println(" getOffsetFetchResponse bytes : ", encoder.Bytes())
	return encoder.Bytes()
}

func getFetchResponse(correlationID uint32, request []byte, clientAddr string) []byte {
	// fmt.Println("Fetch request", request)
	decoder := NewDecoder(request)
	decoder.SkipHeader()
	decoder.offset += 4 + 4 + 4 + 4 + 1 + 4 + 4 // replica_id+ max_wait_ms  + min_bytes +max_bytes +isolation_level + session_id+session_epoch
	nbTopics := decoder.CompactArrayLen()
	topic_partitions := make(map[string][]uint32)
	for i := uint64(0); i < nbTopics; i++ {

		topicName := decoder.String()
		fmt.Println("topicName", topicName)
		nbPartitions := decoder.CompactArrayLen()
		for j := uint64(0); j < nbPartitions; j++ {
			topic_partitions[topicName] = append(topic_partitions[topicName], decoder.UInt32()) //Encoding.Uint32(request[offset:]))
			decoder.offset += 4 + 8 + 4 + 8 + 4                                                 // current_leader_epoch +fetch_offset + last_fetched_epoch + log_start_offset + partition_max_bytes
			decoder.EndStruct()
		}
		decoder.EndStruct()
	}
	// fmt.Println("FETCH topic_partitions", topic_partitions)
	response := FetchResponse{}
	for tp, partitions := range topic_partitions {
		fetchTopicResponse := FetchTopicResponse{TopicName: tp}
		for _, p := range partitions {
			currentOffset := State[clientAddr]
			State[clientAddr]++
			recordBytes, err := GetRecord(uint32(currentOffset), tp, p)
			if err != nil {
				fmt.Printf("Error while fetching record at currentOffset:%v  for topic %v-%v | err: %v", currentOffset, tp, p, err)
			}
			fetchTopicResponse.Partitions = append(fetchTopicResponse.Partitions,
				FetchPartitionResponse{
					PartitionIndex:       p,
					HighWatermark:        uint64(MINUS_ONE), //uint64(MINUS_ONE),
					LastStableOffset:     uint64(MINUS_ONE),
					LogStartOffset:       0,
					PreferredReadReplica: 1,
					Records:              recordBytes, // []byte(line),
				})
		}
		response.Responses = append(response.Responses, fetchTopicResponse)
	}
	fmt.Println("FetchResponse", response)
	encoder := NewEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(response.ThrottleTimeMs)
	encoder.PutInt16(response.ErrorCode)
	encoder.PutInt32(response.SessionId)
	encoder.PutCompactArrayLen(len(response.Responses))
	for _, r := range response.Responses {
		encoder.PutString(r.TopicName)
		encoder.PutCompactArrayLen(len(r.Partitions))
		for _, p := range r.Partitions {
			encoder.PutInt32(p.PartitionIndex)
			encoder.PutInt16(p.ErrorCode)
			encoder.PutInt64(p.HighWatermark)
			encoder.PutInt64(p.LastStableOffset)
			encoder.PutInt64(p.LogStartOffset)

			encoder.PutCompactArrayLen(len(p.AbortedTransactions))
			encoder.PutInt32(p.PreferredReadReplica)
			// encoder.PutCompactArrayLen(len(p.Records)) // already included
			encoder.PutBytes(p.Records)
			encoder.EndStruct()
		}
		encoder.EndStruct()
	}
	encoder.EndStruct()
	encoder.PutLen()
	fmt.Println(" getFetchResponse bytes : ", encoder.Bytes())
	return encoder.Bytes()
}
func handleConnection(conn net.Conn) {
	defer conn.Close() // Ensure the connection is closed when the function exits

	// Print client information
	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("Connection established with %s\n", clientAddr)
	State[clientAddr] = 0
	// Read data from the connection
	for {
		buffer := make([]byte, 1024)
		// Read incoming data
		_, err := conn.Read(buffer)
		// fmt.Printf("n, err, buffer,string(buffer) : %v, %v, %v, %v /n/n", n, err, buffer, string(buffer))
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Printf("Error reading from connection: %v\n", err)
			}
			break
		}
		// Send a response back to the client
		requestApikey := Encoding.Uint16(buffer[4:])
		correlationID := Encoding.Uint32(buffer[8:])
		fmt.Printf("requestApikey: %v, correlationID: %v \n\n", requestApikey, correlationID)

		// if correlationID > 5e10 {
		// 	os.Exit(1)
		// }

		var response []byte
		switch requestApikey {
		case ProduceKey:
			response = getProduceResponse(correlationID, buffer)
		case FetchKey:
			response = getFetchResponse(correlationID, buffer, clientAddr)
		case MetadataKey:
			response = getMetadataResponse(correlationID)
		case APIVersionKey:
			response = getAPIVersionResponse(correlationID)
		case CreateTopicKey:
			response = getCreateTopicResponse(correlationID, buffer)
		case InitProducerIdKey:
			response = getInitProducerIdResponse(correlationID, buffer)
		case FindCoordinatorKey:
			response = getFindCoordinatorResponse(correlationID, buffer)
		case JoinGroupKey:
			response = getJoinGroupResponse(correlationID, buffer)
		case HeartbeatKey:
			response = getHeartbeatResponse(correlationID, buffer)
		case SyncGroupKey:
			response = getSyncGroupResponse(correlationID, buffer)
		case OffsetFetchKey:
			response = getOffsetFetchResponse(correlationID, buffer)

		}

		_, err = conn.Write(response)
		// fmt.Printf("sent back %v bytes \n\n", N)
		if err != nil {
			fmt.Printf("Error writing to connection: %v\n", err)
			break
		}
		clear(buffer)
	}

	fmt.Printf("Connection with %s closed.\n", clientAddr)
}

func main() {
	// Set up a TCP listener on port 9092
	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		fmt.Printf("Error starting server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Server is listening on port 9092...")

	for {
		// Accept a new client connection
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		// Handle the new connection in a new goroutine
		go handleConnection(conn)
	}
}
