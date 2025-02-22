package protocol

import (
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/CefBoud/monkafka/logging"
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/raft"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
	"github.com/CefBoud/monkafka/utils"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
)

const (
	// serfEventChSize is the size of the buffered channel to get Serf
	// events. If this is exhausted we will block Serf and Memberlist.
	serfEventChSize = 2048
)

// Broker represents a Kafka broker instance
type Broker struct {
	Config         *types.Configuration
	ShutDownSignal chan bool
	Serf           *serf.Serf  // Serf cluster maintained inside the DC
	Raft           *hraft.Raft // Raft cluster maintained inside the DC
	FSM            *raft.FSM

	RaftNotifyCh <-chan bool // raftNotifyCh ensures that we get reliable leader transition notifications from the Raft layer.

	SerfEventCh chan serf.Event  // eventCh is used to receive events from the serf cluster
	ReconcileCh chan serf.Member //  used to pass events from the serf handler into the leader manager to update the strong state
}

// NewBroker creates a new Broker instance with the provided configuration
func NewBroker(config *types.Configuration) *Broker {
	return &Broker{
		Config:         config,
		ShutDownSignal: make(chan bool),
		RaftNotifyCh:   make(<-chan bool),
		SerfEventCh:    make(chan serf.Event, serfEventChSize),
		ReconcileCh:    make(chan serf.Member),
	}
}

// Startup initializes the broker, starts the storage, loads group metadata,
// and listens for incoming connections
func (b *Broker) Startup() {
	var err error
	b.Config.LogDir = filepath.Join(b.Config.LogDir, fmt.Sprintf("MonKafka-%v", b.Config.NodeID))
	b.FSM = &raft.FSM{NodeID: uint32(b.Config.NodeID), Topics: make(map[string]types.Topic), Nodes: make(map[uint32]types.Node)}

	err = b.SetupRaft()
	if err != nil {
		log.Panic("Raft Setup failed: %v", err)
	}

	err = b.SetupSerf()
	if err != nil {
		log.Panic("Serf Setup failed: %v", err)
	}

	go b.handleSerfEvent()
	go b.monitorLeadership()
	go b.printClusteringInfo()

	storage.Startup(b.Config, b.ShutDownSignal)
	LoadGroupMetadataState()

	// Set up a TCP listener on the specified port
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", b.Config.BrokerPort))
	if err != nil {
		log.Error("Error starting server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	log.Info("Server is listening on port %d...\n", b.Config.BrokerPort)

	// Accept and handle incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Error("Error accepting connection: %v\n", err)
			continue
		}
		go b.HandleConnection(conn)
	}
}

// HandleConnection processes incoming requests from a client connection
func (b *Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	connectionAddr := conn.RemoteAddr().String()
	log.Debug("Connection established with %s\n", connectionAddr)

	for {
		// First, we read the length, then allocate a byte slice based on it.
		// ReadFull (not Read) is used to ensure the entire request is read. Partial data would result in parsing errors
		lengthBuffer := make([]byte, 4)
		_, err := io.ReadFull(conn, lengthBuffer)
		if err != nil {
			log.Info("failed to read request's length. Error: %v ", err)
			return
		}
		length := serde.Encoding.Uint32(lengthBuffer)
		buffer := make([]byte, length+4)
		copy(buffer, lengthBuffer)
		_, err = io.ReadFull(conn, buffer[4:])
		if err != nil {
			if err.Error() != "EOF" {
				log.Error("Error reading from connection: %v\n", err)
			}
			break
		}
		req := serde.ParseHeader(buffer, connectionAddr)
		apiKeyHandler := b.APIDispatcher(req.RequestAPIKey)
		log.Debug("Received RequestAPIKey: %v | RequestAPIVersion: %v | CorrelationID: %v | Length: %v \n\n", apiKeyHandler.Name, req.RequestAPIVersion, req.CorrelationID, length)
		response := apiKeyHandler.Handler(req)

		_, err = conn.Write(response)
		if err != nil {
			log.Error("Error writing to connection: %v\n", err)
			break
		}
	}
	log.Debug("Connection with %s closed.\n", connectionAddr)
}

// Shutdown gracefully shuts down the broker and its components
func (b *Broker) Shutdown() {
	// close ShutDownSignal so any goroutine waiting on it will run
	close(b.ShutDownSignal)
	log.Info("Broker Shut down...")
	log.Info("Shutting down Serf ...")

	log.Info("Waiting a bit after Serf leaving to allow other servers to be notified")
	if b.IsController() { // raft leader
		raftServers, err := b.getRaftServers()
		if err != nil {
			log.Error("failed to get raft server %v", err)
		} else {
			if len(raftServers) > 2 {
				log.Info("Node is raft leader and there are >2 raft servers, calling LeadershipTransfer")
				future := b.Raft.RemoveServer(hraft.ServerID(b.Config.RaftID), 0, 0)
				if err := future.Error(); err != nil {
					log.Error("failed to remove self from raft cluster %v", err)
				}
			}
		}
	}

	if err := b.Serf.Leave(); err != nil {
		log.Error("Serf leave failed: %s", err)
	}
	LeaveDrainTime := 5 * time.Second

	time.Sleep(LeaveDrainTime)
	storage.Shutdown()

	if b.Serf != nil {
		b.Serf.Shutdown()
	}

	if b.Raft != nil {
		future := b.Raft.Shutdown()
		if err := future.Error(); err != nil {
			log.Warn("error shutting down raft:  %v", err)
		}
	}
}

func (b *Broker) getRaftServers() ([]hraft.Server, error) {
	configFuture := b.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, fmt.Errorf("getRaftServer: can't get raft configuration error: %s", err)
	}
	return configFuture.Configuration().Servers, nil
}

// AppendRaftEntry add a new entry to the raft log
func (b *Broker) AppendRaftEntry(entryType raft.CommandType, entry any) (any, error) {
	bytes, err := raft.EncodeLogEntry(entryType, entry)
	if err != nil {
		return nil, err
	}
	future := b.Raft.Apply(bytes, 10*time.Second)
	if err := future.Error(); err != nil {
		return nil, err
	}
	log.Info("added entry to raft: %+v", entry)
	return future.Response(), nil
}

// IsController return if the broker is the cluster's controller which is also the raft leader
func (b *Broker) IsController() bool {
	return b.Raft.State() == hraft.Leader
}

// SetupRaft inits Raft for the broker
func (b *Broker) SetupRaft() error {
	raftAddress := b.Config.RaftAddress // fmt.Sprintf("localhost:%v", b.Config.RaftPort)
	dir := path.Join(b.Config.LogDir, "raft"+b.Config.RaftID)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create data directory: %s", err)
	}

	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))
	if err != nil {
		return fmt.Errorf("could not create bolt store: %s", err)
	}

	snapshots, err := hraft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("could not create snapshot store: %s", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return fmt.Errorf("could not resolve address: %s", err)
	}

	transport, err := hraft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return fmt.Errorf("could not create tcp transport: %s", err)
	}

	raftCfg := hraft.DefaultConfig()
	raftCfg.LogLevel = "INFO"

	if b.Config.RaftID == "" {
		b.Config.RaftID = fmt.Sprintf("raft-broker-%d", b.Config.NodeID)
	}
	nodeID := b.Config.RaftID
	raftCfg.LocalID = hraft.ServerID(nodeID)

	// Set up a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	raftCfg.NotifyCh = raftNotifyCh
	b.RaftNotifyCh = raftNotifyCh

	b.Raft, err = hraft.NewRaft(raftCfg, b.FSM, store, store, snapshots, transport)
	if err != nil {
		return fmt.Errorf("could not create raft instance: %s", err)
	}

	if b.Config.Bootstrap {
		logging.Info("bootstrapping raft with nodeID %v ....", nodeID)
		hasState, err := hraft.HasExistingState(store, store, snapshots)
		if err != nil {
			return err
		}
		logging.Info("bootstrapping hasState %v", hasState)
		if !hasState {
			future := b.Raft.BootstrapCluster(hraft.Configuration{
				Servers: []hraft.Server{
					{
						ID:      hraft.ServerID(nodeID),
						Address: transport.LocalAddr(),
					},
				},
			})
			if err := future.Error(); err != nil {
				logging.Error(" bootstrap cluster error: %s", err)
			}
		}
	}
	return nil
}

// SetupSerf to setup the serf agent and maybe join a serf cluster
func (b *Broker) SetupSerf() error {
	var err error
	conf := b.Config.SerfConfig
	conf.Init()
	conf.NodeName = b.Config.RaftID
	bindIP, bindPort, err := net.SplitHostPort(b.Config.SerfAddress)
	if err != nil {
		return err
	}
	log.Debug("SetupSerf: bindIP=%v bindPort=%v", bindIP, bindPort)
	conf.MemberlistConfig.BindAddr = bindIP
	conf.MemberlistConfig.BindPort, err = strconv.Atoi(bindPort)
	if err != nil {
		return err
	}
	conf.Tags["role"] = "broker"
	conf.Tags["ID"] = strconv.Itoa(b.Config.NodeID)
	conf.Tags["broker_addr"] = fmt.Sprintf("%s:%d", b.Config.BrokerHost, b.Config.BrokerPort)
	conf.Tags["raft_server_id"] = b.Config.RaftID
	conf.Tags["raft_addr"] = b.Config.RaftAddress
	conf.Tags["serf_addr"] = b.Config.SerfAddress

	conf.EventCh = b.SerfEventCh
	conf.SnapshotPath = filepath.Join(b.Config.LogDir, "serf-snapshot")

	if err = utils.EnsurePath(conf.SnapshotPath, false); err != nil {
		return fmt.Errorf("could not serf SnapshotPath dir: %s", err)
	}

	b.Serf, err = serf.Create(conf)

	if len(b.Config.SerfJoinAddress) > 0 {
		existingSerfNodes := strings.Split(b.Config.SerfJoinAddress, ",")
		log.Info("joining serf nodes: %v", existingSerfNodes)
		n, err := b.Serf.Join(existingSerfNodes, true)
		if err != nil {
			log.Error("Couldn't join cluster, starting own: %v\n", err)
		} else {
			log.Info("Serf join: successfully contacted %v node. Members: %v", n, b.Serf.Members())
		}
	}
	return err
}

func (b *Broker) handleSerfEvent() {
	for {
		log.Info("handleSerfEvent .... %v", b.Serf.Memberlist().Members())
		select {
		case e := <-b.SerfEventCh:
			log.Info("serf EventType: %v", e.EventType())
			switch e.EventType() {
			case serf.EventMemberJoin:
				b.handleSerfMemberJoin(e.(serf.MemberEvent))
			case serf.EventMemberFailed:
				// a node is moved from `fail` to `reap` (completely ousted from the cluster) after `reconnect_timeout` (defaults to 24h)
				log.Error("handleSerfEvent... EventMemberFailed ")
			case serf.EventMemberReap, serf.EventMemberLeave:
				b.handleSerfMemberLeft(e.(serf.MemberEvent))
			}
		case <-b.ShutDownSignal:

			return
		}
	}
}

// GetClusterNodes returns the raft cluster nodes each representing a broker
func (b *Broker) GetClusterNodes() ([]*types.Node, error) {
	configFuture := b.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Error("handleSerfMemberLeft: can't get raft configuration error: %s", err)
		return nil, err
	}
	nodes := make(map[string]*types.Node)

	for _, server := range configFuture.Configuration().Servers {
		nodes[string(server.ID)] = &types.Node{}
	}

	_, leaderID := b.Raft.LeaderWithID()
	for _, m := range b.Serf.Members() {
		raftServerID := m.Tags["raft_server_id"]
		n, ok := nodes[raftServerID]
		if ok {
			// log.Debug("serf node tags %v", m.Tags)
			id, err := strconv.Atoi(m.Tags["ID"])
			if err != nil {
				log.Error("GetClusterNodes: unable to convert serf ID to uint32: %v", err)
				continue
			}
			n.NodeID = uint32(id)
			host, port, err := net.SplitHostPort(m.Tags["broker_addr"])
			if err != nil {
				log.Error("GetClusterNodes: unable to parse broker_addr: %v", err)
				continue
			}
			portInt, _ := strconv.Atoi(port)
			n.Host, n.Port = host, uint32(portInt)
			n.IsController = string(leaderID) == raftServerID
		}
	}
	var res []*types.Node
	for _, n := range nodes {
		res = append(res, n)
	}
	return res, nil
}

func (b *Broker) printClusteringInfo() {
	ticker := time.NewTicker(30000 * time.Millisecond)
	defer ticker.Stop() // Ensure the ticker is stopped when done
	for {
		select {
		case <-ticker.C:
			leaderAddr, leaderID := b.Raft.LeaderWithID()
			log.Debug("Serf members:  %v", b.Serf.Members())
			log.Debug("Raft LeaderAddr: [%v] - leaderID [%v]", leaderAddr, leaderID)
		}
	}
}
