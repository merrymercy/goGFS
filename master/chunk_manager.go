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
	chunk map[llgfs.ChunkHandle]chunkInfo
}

type chunkInfo struct {
	sync.RWMutex
	location util.ArraySet       // set of replica locations
	primary  llgfs.ServerAddress // primary chunkserver
	expire   time.Time           // lease expire time
}

// AddReplica adds a replica for a chunk
func (cm *chunkManager) AddReplica(handle llgfs.ChunkHandle, addr llgfs.ServerAddress) error {
	c, ok := cm.chunk[handle]
	if !ok {
		return fmt.Errorf("chunk %v not found", handle)
	}
	c.Lock()
	defer c.Unlock()
	c.location.Add(addr)
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle llgfs.ChunkHandle) (*util.ArraySet, error) {
	c, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}
	c.Lock()
	defer c.Unlock()
	return &c.location, nil
}

// GetLessee returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLessee(handle llgfs.ChunkHandle) (*lessee, error) {
	c, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}
	c.Lock()
	defer c.Unlock()

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
	c, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}
	c.Lock()
	defer c.Unlock()

	now := time.Now()
	if c.primary != primary && c.expire.After(now) {
		return nil, fmt.Errorf("%v does not hold the lease for chunk %v", primary, handle)
	}
	c.primary = primary
	c.expire = now.Add(llgfs.LeaseExpire)
	return &c.expire, nil
}
