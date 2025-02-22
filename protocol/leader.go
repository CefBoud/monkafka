package protocol

import (
	log "github.com/CefBoud/monkafka/logging"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

func (b *Broker) handleSerfMemberJoin(event serf.MemberEvent) error {
	_, leaderID := b.Raft.LeaderWithID()
	noLeader := leaderID == ""
	if noLeader {
		if b.Config.Bootstrap {
			log.Info("handleSerfMemberJoin: there is no leader, current node will bootstrap")
			// if bootstrapping, but no yet controller, we give raft state some time to transition to leader
			// if !b.IsController() {
			// 	log.Info("Waiting for node to bootstrap raft and become leader")
			// 	timeToWaitForLeaderShip := time.Now().Add(5 * time.Second)
			// 	leader := false
			// 	for !leader && time.Now().Before(timeToWaitForLeaderShip) {
			// 		time.Sleep(50 * time.Millisecond)
			// 		leader = b.IsController()
			// 	}
			// 	log.Info("finished waiting. Is Node leader ? %v", leader)

			// }
		} else {
			log.Info("handleSerfMemberJoin: there is no leader, current node WILL NOT bootstrap")
			return nil
		}
	} else {
		if !b.IsController() {
			log.Info("handleSerfMemberJoin: node is not the leader, ignoring join event")
			return nil
		}
	}

	newMembers := make(map[string]serf.Member)
	for _, m := range event.Members {
		if m.Tags["role"] != "broker" {
			log.Info("handleSerfMemberJoin: new member [%v - %v] is not a broker", m.Name, m.Addr)
			continue
		}
		newMembers[m.Tags["raft_addr"]] = m
	}

	raftServers, err := b.getRaftServers()
	if err != nil {
		return err
	}
	for _, server := range raftServers {
		for raftAddr := range newMembers {
			if server.Address == raft.ServerAddress(raftAddr) {
				log.Info("handleSerfMemberJoin: member [%v] already in raft cluster", raftAddr)
				delete(newMembers, raftAddr)
				if len(newMembers) == 0 {
					return nil
				}
			}
		}
	}
	for raftAddr, m := range newMembers {
		log.Info("handleSerfMemberJoin: adding voter to the raft cluster with addr %s", raftAddr)
		err := b.Raft.AddVoter(raft.ServerID(m.Tags["raft_server_id"]), raft.ServerAddress(m.Tags["raft_addr"]), 0, 0).Error()
		if err != nil {
			log.Error("Failed to add follower: %s", err)
			return err
		}
	}
	return nil
}

func (b *Broker) handleSerfMemberLeft(event serf.MemberEvent) error {
	log.Debug("Inside handleSerfMemberLeft")
	_, leaderID := b.Raft.LeaderWithID()
	if leaderID == "" {
		log.Info("handleSerfMemberLeft: there is no leader. Nothing to do.")
		return nil
	} else if !b.IsController() {
		log.Info("handleSerfMemberLeft: node is not the leader, ignoring left/reap/failed event")
		return nil
	}

	eventMembers := make(map[string]serf.Member)
	for _, m := range event.Members {
		if m.Tags["role"] != "broker" {
			log.Info("handleSerfMemberLeft: member [%v - %v] is not a broker", m.Name, m.Addr)
			continue
		}
		eventMembers[m.Tags["raft_addr"]] = m
	}

	raftServers, err := b.getRaftServers()
	if err != nil {
		return err
	}
	for _, server := range raftServers {
		for raftAddr := range eventMembers {
			if server.Address == raft.ServerAddress(raftAddr) {
				log.Info("handleSerfMemberLeft: removing member [%v] from raft cluster", raftAddr)

				future := b.Raft.RemoveServer(server.ID, 0, 0)
				if err := future.Error(); err != nil {
					log.Error("handleSerfMemberLeft: remove server [%v] from raft error: %s", server.Address, err)
					return err
				}
			}
		}
	}
	return nil
}

func (b *Broker) monitorLeadership() {

	// var weAreLeaderCh chan struct{}
	// var leaderLoop sync.WaitGroup
	for {
		log.Info("monitorLeadership loop")
		select {
		case isLeader := <-b.RaftNotifyCh:
			servers, err := b.getRaftServers()
			log.Info("monitorLeadership isLeader: %v  | servers: %+v %v", isLeader, servers, err)

		}
	}
}
