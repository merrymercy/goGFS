package master

import (
	"fmt"
	//"math/rand"
	"sync"
	"time"

	"gfs"
	"gfs/util"
	log "github.com/Sirupsen/logrus"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[gfs.ServerAddress]*chunkServerInfo),
	}
	log.Info("-----------new chunk server manager")
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
	garbage       []gfs.ChunkHandle
}

func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress, reply *gfs.HeartbeatReply) bool {
	csm.Lock()
	defer csm.Unlock()

	sv, ok := csm.servers[addr]
	if !ok {
		log.Info("New chunk server" + addr)
		csm.servers[addr] = &chunkServerInfo{time.Now(), make(map[gfs.ChunkHandle]bool), nil}
		return true
	} else {
		// send garbage
		reply.Garbage = csm.servers[addr].garbage
		csm.servers[addr].garbage = make([]gfs.ChunkHandle, 0)
		sv.lastHeartbeat = time.Now()
		return false
	}
}

// register a chunk to servers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()

	for _, v := range addrs {
		//csm.servers[v].chunks[handle] = true
		sv, ok := csm.servers[v]
		if ok {
			sv.chunks[handle] = true
		} else {
			log.Warning("add chunk in removed server ", sv)
		}
	}
}

// AddGarbage
func (csm *chunkServerManager) AddGarbage(addr gfs.ServerAddress, handle gfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()

	sv, ok := csm.servers[addr]
	if ok {
		sv.garbage = append(sv.garbage, handle)
	}
}

// ChooseReReplication chooses servers to perfomr re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	from = ""
	to = ""
	err = nil
	for a, v := range csm.servers {
		if v.chunks[handle] {
			from = a
		} else {
			to = a
		}
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("No enough server for replica %v", handle)
	return
}

// ChooseServers returns servers to store new chunk
// called when a new chunk is create
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {

	if num > len(csm.servers) {
		return nil, fmt.Errorf("no enough servers for %v replicas", num)
	}

	csm.RLock()
	var all, ret []gfs.ServerAddress
	for a, _ := range csm.servers {
		all = append(all, a)
	}
	csm.RUnlock()

	choose, err := util.Sample(len(all), num)
	if err != nil {
		return nil, err
	}
	for _, v := range choose {
		ret = append(ret, all[v])
	}

	return ret, nil
}

// DetectDeadServers detect disconnected servers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() []gfs.ServerAddress {
	csm.RLock()
	defer csm.RUnlock()

	var ret []gfs.ServerAddress
	now := time.Now()
	for k, v := range csm.servers {
		if v.lastHeartbeat.Add(gfs.ServerTimeout).Before(now) {
			ret = append(ret, k)
		}
	}

	return ret
}

// RemoveServers removes metedata of disconnected server
// it returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Unlock()

	err = nil
	sv, ok := csm.servers[addr]
	if !ok {
		err = fmt.Errorf("Cannot find chunk server %v", addr)
		return
	}
	for h, v := range sv.chunks {
		if v {
			handles = append(handles, h)
		}
	}
	delete(csm.servers, addr)

	return
}
