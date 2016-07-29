package master

import (
    "fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/abcdabcd987/llgfs/gfs"
	"github.com/abcdabcd987/llgfs/gfs/util"
)

// chunkManager manges chunks
type chunkManager struct {
	lock      sync.RWMutex
	batchLock bool

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	location util.ArraySet     // set of replica locations
	primary  gfs.ServerAddress // primary chunkserver
	expire   time.Time         // lease expire time
}

type fileInfo struct {
	handles []gfs.ChunkHandle
}

type lease struct {
	primary     gfs.ServerAddress
	expire      time.Time
	secondaries []gfs.ServerAddress
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
        chunk: make(map[gfs.ChunkHandle]*chunkInfo),
        file : make(map[gfs.Path]*fileInfo),
	}
    log.Info("---------new chunk manager")
	return cm
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
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
    cm.Lock()
    defer cm.Unlock()

    chunkinfo, ok := cm.chunk[handle]
    if !ok {
        return fmt.Errorf("cannot find chunk %d", int64(handle))
    }
    chunkinfo.location.Add(addr)
    return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) (*util.ArraySet, error) {
    cm.RLock()
    defer cm.RUnlock()

    chunkinfo, ok := cm.chunk[handle]
    if !ok {
        return nil, fmt.Errorf("cannot find chunk %d", int64(handle))
    }
    return &chunkinfo.location, nil
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
    cm.RLock()
    defer cm.RUnlock()

    fileinfo, ok := cm.file[path]
    if !ok {
        return -1, fmt.Errorf("cannot get handle for %v[%v]", path, index)
    }

    if index < 0 || int(index) >= len(fileinfo.handles) {
        return -1, fmt.Errorf("Invalid index for %v[%v]", path, index)
    }

    return fileinfo.handles[index], nil
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*lease, error) {
    cm.RLock()
    defer cm.RUnlock()

    ret := &lease{}
    chunkinfo, ok := cm.chunk[handle]
    if !ok { return nil, fmt.Errorf("invalid chunk handle %v", handle) }

    if chunkinfo.expire.Before(time.Now()) { // grants a new lease
        chunkinfo.primary = chunkinfo.location.RandomPick().(gfs.ServerAddress)
        chunkinfo.expire = time.Now().Add(gfs.LeaseExpire)
    }

    ret.primary = chunkinfo.primary
    ret.expire = chunkinfo.expire
    for _, v := range chunkinfo.location.GetAll() {
        if vv := v.(gfs.ServerAddress); vv != chunkinfo.primary {
            ret.secondaries = append(ret.secondaries, vv)
        }
    }
    return ret, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) (*time.Time, error) {
    cm.Lock()
    defer cm.Unlock()

    chunkinfo, ok := cm.chunk[handle]
    if !ok { return nil, fmt.Errorf("invalid chunk handle %v", handle) }

    // TODO
    _ = chunkinfo

    return nil, nil
}

// CreateChunk creates a new chunk for path. The index must be the next chunk of the path.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, error) {
    cm.Lock()
    defer cm.Unlock()

    handle := cm.numChunkHandle
    cm.numChunkHandle++

    // update file info
    fileinfo, ok := cm.file[path];
    if !ok {
        fileinfo = new(fileInfo)
        cm.file[path] = fileinfo
    }
    fileinfo.handles = append(fileinfo.handles, handle)

    // update chunk info
    cm.chunk[handle] = new(chunkInfo)

    return handle, nil
}
