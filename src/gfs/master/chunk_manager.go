package master

import (
	"fmt"
	"sync"
	"time"

	"gfs"
	"gfs/util"
	log "github.com/Sirupsen/logrus"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	replicasNeedList []gfs.ChunkHandle // list of handles need a new replicas
	// (happends when some servers are disconneted)
	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location []gfs.ServerAddress // set of replica locations
	primary  gfs.ServerAddress   // primary chunkserver
	expire   time.Time           // lease expire time
	version  gfs.ChunkVersion
	checksum gfs.Checksum
	path     gfs.Path
}

type fileInfo struct {
	sync.RWMutex
	handles []gfs.ChunkHandle
}

type serialChunkInfo struct {
	Path gfs.Path
	Info []gfs.PersistentChunkInfo
}

func (cm *chunkManager) Deserialize(files []serialChunkInfo) error {
	cm.RLock()
	defer cm.RUnlock()

	now := time.Now()
	for _, v := range files {
		log.Info("Master restore files ", v.Path)
		f := new(fileInfo)
		for _, ck := range v.Info {
			f.handles = append(f.handles, ck.Handle)
			log.Info("Master restore chunk ", ck.Handle)
			cm.chunk[ck.Handle] = &chunkInfo{
				expire:   now,
				version:  ck.Version,
				checksum: ck.Checksum,
			}
		}
		cm.numChunkHandle += gfs.ChunkHandle(len(v.Info))
		cm.file[v.Path] = f
	}

	return nil
}

func (cm *chunkManager) Serialize() []serialChunkInfo {
	cm.RLock()
	defer cm.RUnlock()

	var ret []serialChunkInfo
	for k, v := range cm.file {
		var chunks []gfs.PersistentChunkInfo
		for _, handle := range v.handles {
			chunks = append(chunks, gfs.PersistentChunkInfo{
				Handle:   handle,
				Length:   0, // TODO
				Version:  0,
				Checksum: 0,
			})
		}

		ret = append(ret, serialChunkInfo{Path: k, Info: chunks})
	}

	return ret
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	log.Info("-----------new chunk manager")
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	ck.Lock()
	defer ck.Unlock()
	ck.location = append(ck.location, addr)
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	cm.RLock()
	defer cm.RUnlock()

	ck, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("cannot find chunk %v", handle)
	}
	return ck.location, nil
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
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()

	ret := &gfs.Lease{}
	if !ok {
		return nil, fmt.Errorf("invalid chunk handle %v", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	if ck.expire.Before(time.Now()) { // grants a new lease
		ck.primary = ck.location[0]
		ck.expire = time.Now().Add(gfs.LeaseExpire)
	}

	ret.Primary = ck.primary
	ret.Expire = ck.expire
	for _, v := range ck.location {
		if v != ck.primary {
			ret.Secondaries = append(ret.Secondaries, v)
		}
	}
	return ret, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return fmt.Errorf("invalid chunk handle %v", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	now := time.Now()
	if ck.primary != primary && ck.expire.After(now) {
		return fmt.Errorf("%v does not hold the lease for chunk %v", primary, handle)
	}
	ck.primary = primary
	ck.expire = now.Add(gfs.LeaseExpire)
	return nil
}

// CreateChunk creates a new chunk for path. servers for the chunk are denoted by addrs
// returns the handle of the new chunk, and the servers that create the chunk successfully
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, []gfs.ServerAddress, error) {
	cm.Lock()

	handle := cm.numChunkHandle
	cm.numChunkHandle++

	// update file info
	fileinfo, ok := cm.file[path]
	if !ok {
		fileinfo = new(fileInfo)
		cm.file[path] = fileinfo
	}
	fileinfo.handles = append(fileinfo.handles, handle)

	// update chunk info
	ck := &chunkInfo{path: path}
	cm.chunk[handle] = ck
	cm.Unlock()

	var errList string
	var success []gfs.ServerAddress
	for _, v := range addrs {
		var r gfs.CreateChunkReply
		err := util.Call(v, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{handle}, &r)
		if err == nil { // register
			ck.location = append(ck.location, v)
			success = append(success, v)
		} else {
			errList += err.Error() + ";"
		}
	}

	if errList == "" {
		return handle, success, nil
	} else {
		return handle, success, fmt.Errorf(errList)
	}
}

// RemoveChunks removes disconnected chunks
// if replicas number of a chunk is less than gfs.MininumNumReplicas, add it to need list
func (cm *chunkManager) RemoveChunks(handles []gfs.ChunkHandle, server gfs.ServerAddress) error {
	for _, v := range handles {
		ck := cm.chunk[v]
		ck.Lock()
		// delete element in slice
		for i, vv := range ck.location {
			if vv == server {
				ck.location = append(ck.location[:i], ck.location[i+1:]...)
			}
		}
		ck.expire = time.Now()

		if len(ck.location) == 0 {
			ck.Unlock()
			return fmt.Errorf("Lose all replicas of chunk %v", v)
		} else if len(ck.location) < gfs.MinimumNumReplicas {
			cm.Lock()
			cm.replicasNeedList = append(cm.replicasNeedList, v)
			cm.Unlock()
		}
		ck.Unlock()
	}
	return nil
}

// GetNeedList clears the need list at first (removes the old handles that nolonger need replicas)
// and then return all new handles
func (cm *chunkManager) GetNeedlist() []gfs.ChunkHandle {
	cm.Lock()
	defer cm.Unlock()

	// clear list
	var newlist []gfs.ChunkHandle
	for _, v := range cm.replicasNeedList {
		if len(cm.chunk[v].location) < gfs.MinimumNumReplicas {
			newlist = append(newlist, v)
		}
	}
	cm.replicasNeedList = newlist

	if len(cm.replicasNeedList) > 0 {
		return cm.replicasNeedList
	} else {
		return nil
	}
}
