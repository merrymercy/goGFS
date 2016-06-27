package master

import (
	"sync"
	"time"

	"github.com/abcdabcd987/llgfs"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	lock    sync.RWMutex
	servers map[llgfs.ServerAddress]chunkServerInfo
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[llgfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

func (csm *chunkServerManager) Heartbeat(addr llgfs.ServerAddress) {
	csm.lock.Lock()
	defer csm.lock.Unlock()

	cs := csm.servers[addr]
	cs.lastHeartbeat = time.Now()
}

func (csm *chunkServerManager) AddChunks(addr llgfs.ServerAddress, chunks []llgfs.ChunkHandle) {
	csm.lock.Lock()
	defer csm.lock.Unlock()

	cs := csm.servers[addr]
	for _, v := range chunks {
		cs.chunks[v] = true
	}
}

func (csm *chunkServerManager) Sample(k int) ([]llgfs.ServerAddress, error) {
	csm.lock.RLock()
	defer csm.lock.RUnlock()

}
