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
	lock sync.RWMutex

	chunk map[llgfs.ChunkHandle]chunkInfo // chunk handle -> chunkInfo
	file  map[filePair]llgfs.ChunkHandle  // (path, chunk index) -> chunk handle

	nextChunkHandle llgfs.ChunkHandle
}

type filePair struct {
	path  string // path of the file
	index int64  // chunk index
}

type chunkInfo struct {
	sync.RWMutex
	location util.ArraySet       // set of replica locations
	primary  llgfs.ServerAddress // primary chunkserver
	expire   time.Time           // lease expire time
}

// AddReplica adds a replica for a chunk
func (cm *chunkManager) AddReplica(handle llgfs.ChunkHandle, addr llgfs.ServerAddress) error {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	c, ok := cm.chunk[handle]
	if !ok {
		return fmt.Errorf("chunk %v not found", handle)
	}
	c.location.Add(addr)
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle llgfs.ChunkHandle) (*util.ArraySet, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	c, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}
	return &c.location, nil
}

// AddChunk add a chunk for (path, index) with replicas. If there already exists
// a chunk for (path, index), no new chunk will be added.
func (cm *chunkManager) AddChunk(path string, index int64, replicas []llgfs.ServerAddress) llgfs.ChunkHandle {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	p := filePair{path, index}
	h, ok := cm.file[p]
	if ok {
		return h
	}

	c := chunkInfo{}
	for _, rep := range replicas {
		c.location.Add(rep)
	}
	h = cm.nextChunkHandle
	cm.nextChunkHandle++
	cm.file[p] = h
	cm.chunk[h] = c

	return h
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path string, index int64) (llgfs.ChunkHandle, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	h, ok := cm.file[filePair{path, index}]
	if ok {
		return h, nil
	}
	return 0, fmt.Errorf("no chunk for path %s index %d", path, index)
}

// GetLessee returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLessee(handle llgfs.ChunkHandle) (*lessee, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	c, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}

	// if lease expired or no lessee
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

	return &lessee{c.primary, c.expire, snd}, nil
}

// ExtendLease extends the lease of chunk if the lessee is nobody or primary.
func (cm *chunkManager) ExtendLease(handle llgfs.ChunkHandle, primary llgfs.ServerAddress) (*time.Time, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

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
