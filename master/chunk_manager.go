package master

import (
	"fmt"
	"sync"
	"time"

	"github.com/abcdabcd987/llgfs"
	"github.com/abcdabcd987/llgfs/util"
)

// chunkManager manges chunks
type chunkManager struct {
	lock      sync.RWMutex
	batchLock bool

	chunk map[llgfs.ChunkHandle]*chunkInfo
	file  map[llgfs.Path]*fileInfo

	numChunkHandle llgfs.ChunkHandle
}

type chunkInfo struct {
	location util.ArraySet       // set of replica locations
	primary  llgfs.ServerAddress // primary chunkserver
	expire   time.Time           // lease expire time
}

type fileInfo struct {
	index []llgfs.ChunkHandle
}

type lease struct {
	primary     llgfs.ServerAddress
	expire      time.Time
	secondaries []llgfs.ServerAddress
}

func (cm *chunkManager) Lock() {
	cm.lock.Lock()
	cm.batchLock = true
}

func (cm *chunkManager) Unlock() {
	cm.lock.Unlock()
	cm.batchLock = false
}

func (cm *chunkManager) RLock() {
	cm.lock.RLock()
	cm.batchLock = true
}

func (cm *chunkManager) RUnlock() {
	cm.lock.RUnlock()
	cm.batchLock = false
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle llgfs.ChunkHandle, addr llgfs.ServerAddress) error {
	if !cm.batchLock {
		cm.lock.RLock()
		defer cm.lock.RUnlock()
	}

	c, ok := cm.chunk[handle]
	if !ok {
		return fmt.Errorf("chunk %v not found", handle)
	}
	c.location.Add(addr)
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle llgfs.ChunkHandle) (*util.ArraySet, error) {
	if !cm.batchLock {
		cm.lock.RLock()
		defer cm.lock.RUnlock()
	}

	c, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}
	return &c.location, nil
}

// // AddChunk add a chunk for (path, index) with replicas. If there already exists
// // a chunk for (path, index), no new chunk will be added.
// func (cm *chunkManager) AddChunk(path string, index int64, replicas []llgfs.ServerAddress) llgfs.ChunkHandle {
// 	if !cm.batchLock {
// 		cm.lock.RLock()
// 		defer cm.lock.RUnlock()
// 	}

// 	p := filePair{path, index}
// 	h, ok := cm.file[p]
// 	if ok {
// 		return h
// 	}

// 	c := chunkInfo{}
// 	for _, rep := range replicas {
// 		c.location.Add(rep)
// 	}
// 	h = cm.nextChunkHandle
// 	cm.nextChunkHandle++
// 	cm.file[p] = h
// 	cm.chunk[h] = c

// 	return h
// }

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path llgfs.Path, index llgfs.ChunkIndex) (llgfs.ChunkHandle, error) {
	if !cm.batchLock {
		cm.lock.RLock()
		defer cm.lock.RUnlock()
	}

	if f, ok := cm.file[path]; ok && len(f.index) > int(index) {
		return f.index[index], nil
	}
	return 0, fmt.Errorf("no chunk for path %s index %d", path, index)
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle llgfs.ChunkHandle) (*lease, error) {
	if !cm.batchLock {
		cm.lock.RLock()
		defer cm.lock.RUnlock()
	}

	c, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}

	// if lease expired or no lease holder
	now := time.Now()
	if c.expire.Before(now) {
		if c.location.Size() == 0 {
			return nil, fmt.Errorf("no replica available for chunk %v", handle)
		}

		c.primary = c.location.RandomPick().(llgfs.ServerAddress)
		c.expire = now.Add(llgfs.LeaseExpire)
	}

	var snd []llgfs.ServerAddress
	for _, v := range c.location.GetAll() {
		if addr := v.(llgfs.ServerAddress); addr != c.primary {
			snd = append(snd, addr)
		}
	}

	return &lease{c.primary, c.expire, snd}, nil
}

// ExtendLease extends the lease of chunk if the lease holder is nobody or primary.
func (cm *chunkManager) ExtendLease(handle llgfs.ChunkHandle, primary llgfs.ServerAddress) (*time.Time, error) {
	if !cm.batchLock {
		cm.lock.RLock()
		defer cm.lock.RUnlock()
	}

	c, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}

	now := time.Now()
	if c.primary != primary && c.expire.After(now) {
		return nil, fmt.Errorf("%v does not hold the lease for chunk %v", primary, handle)
	}
	c.primary = primary
	c.expire = now.Add(llgfs.LeaseExpire)
	return &c.expire, nil
}

// CreateChunk creates a new chunk for path. The index must be the next chunk of the path.
func (cm *chunkManager) CreateChunk(path llgfs.Path, index llgfs.ChunkIndex) (llgfs.ChunkHandle, error) {
	if !cm.batchLock {
		cm.lock.Lock()
		defer cm.lock.Unlock()
	}

	f, ok := cm.file[path]
	if !ok {
		if index == 0 {
			f = new(fileInfo)
			cm.file[path] = f
		} else {
			return 0, fmt.Errorf("Path %v does not exist", path)
		}
	}
	if int(index) != len(f.index)+1 {
		return 0, fmt.Errorf("Path %v has %v chunks. Refuse to create Chunk %v", path, len(f.index), index)
	}

	handle := cm.numChunkHandle
	cm.numChunkHandle++
	f.index = append(f.index, handle)
	cm.chunk[handle] = new(chunkInfo)
	return handle, nil
}
