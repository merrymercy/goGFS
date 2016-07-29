package master

import (
    "fmt"
	//"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/abcdabcd987/llgfs/gfs"
	"github.com/abcdabcd987/llgfs/gfs/util"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers     map[gfs.ServerAddress]chunkServerInfo
    addressList []gfs.ServerAddress
    roll        int
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
        servers: make(map[gfs.ServerAddress]chunkServerInfo),
	}
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) {
    csm.Lock()
    defer csm.Unlock()

    sv, ok := csm.servers[addr]
    if !ok {
        log.Info("New chunk server" + addr)
        csm.servers[addr] = chunkServerInfo{time.Now(), make(map[gfs.ChunkHandle]bool)}
        csm.addressList = append(csm.addressList, addr)
    } else {
        sv.lastHeartbeat = time.Now()
    }
}

// register chunk to servers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
    for _, v := range addrs {
        csm.servers[v].chunks[handle] = true
    }

    err := util.CallAll(addrs, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{handle})
    return err
}

// get servers to store new chunk
// TODO : allocation strategy and error handle
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {

    if num > len(csm.servers) {
        return nil, fmt.Errorf("no enough servers for %v replicas", num)
    }

    var ret []gfs.ServerAddress
    for i := 0; i < num; i++ {
        addr := csm.addressList[csm.roll]
        ret = append(ret, addr)
        csm.roll = (csm.roll + 1) % len(csm.servers)
    }

    return ret, nil
}
