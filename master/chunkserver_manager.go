package master

import "time"
import "github.com/abcdabcd987/llgfs"

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	servers map[llgfs.ServerAddress]chunkServerInfo
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[llgfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

func (csm *chunkServerManager) Heartbeat(addr llgfs.ServerAddress) {
	cs := csm.servers[addr]
	cs.lastHeartbeat = time.Now()
}

func (csm *chunkServerManager) AddChunks(addr llgfs.ServerAddress, chunks []llgfs.ChunkHandle) {
	cs := csm.servers[addr]
	for _, v := range chunks {
		cs.chunks[v] = true
	}
}
