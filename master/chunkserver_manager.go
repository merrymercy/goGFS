package master

import (
	"fmt"
	"math/rand"
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

	if k > len(csm.servers) {
		return nil, fmt.Errorf("Cannot sample %v from %v servers", k, len(csm.servers))
	}
	srvs := make([]llgfs.ServerAddress, 0, len(csm.servers))
	for k := range csm.servers {
		srvs = append(srvs, k)
	}
	for i := 0; i < k; i++ {
		j := rand.Intn(len(srvs))
		srvs[i], srvs[j] = srvs[j], srvs[i]
	}
	return srvs[:k], nil
}
