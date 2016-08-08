package master

import (
	"encoding/gob"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"net"
	"net/rpc"
	"os"
	"path"
	"time"

	"gfs"
	"gfs/util"
)

// Master Server struct
type Master struct {
	address    gfs.ServerAddress // master server address
	serverRoot string
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to ture if server is shuntdown

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

const (
	MetaFileName = "gfs-master.meta"
	FilePerm     = 0755
)

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Master {
	m := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

	m.initMetadata()

	// RPC Handler
	go func() {
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				if !m.dead {
					log.Fatal("master accept error:", err)
				}
			}
		}
	}()

	// Background Task
	// BackgroundActivity does all the background activities
	// server disconnection handle, garbage collection, stale replica detection, etc
	go func() {
		checkTicker := time.Tick(gfs.ServerCheckInterval)
		storeTicker := time.Tick(gfs.MasterStoreInterval)
		for {
			var err error
			select {
			case <-m.shutdown:
				return
			case <-checkTicker:
				err = m.serverCheck()
			case <-storeTicker:
				err = m.storeMeta()
			}
			if err != nil {
				log.Warning("Background error ", err)
			}
		}

	}()

	log.Infof("Master is running now. addr = %v", address)

	return m
}

// InitMetadata initiates meta data
func (m *Master) initMetadata() {
	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()
	m.loadMeta()
	return
}

type PersistentBlock struct {
	NamespaceTree []serialTreeNode
	ChunkInfo     []serialChunkInfo
}

// loadMeta loads metadata from disk
func (m *Master) loadMeta() error {
	filename := path.Join(m.serverRoot, MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentBlock
	dec := gob.NewDecoder(file)
	err = dec.Decode(&meta)
	if err != nil {
		return err
	}

	m.nm.Deserialize(meta.NamespaceTree)
	m.cm.Deserialize(meta.ChunkInfo)

	return nil
}

// storeMeta stores metadata to disk
func (m *Master) storeMeta() error {
	filename := path.Join(m.serverRoot, MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentBlock

	meta.NamespaceTree = m.nm.Serialize()
	meta.ChunkInfo = m.cm.Serialize()

	log.Infof("Master : store metadata")
	enc := gob.NewEncoder(file)
	err = enc.Encode(meta)
	return err
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	if !m.dead {
		log.Warning(m.address, " Shutdown")
		m.dead = true
		close(m.shutdown)
		m.l.Close()
	}

	err := m.storeMeta()
	if err != nil {
		log.Warning("error in store metadeta: ", err)
	}
}

// serverCheck checks all chunkserver according to last heartbeat time
// then removes all the information of the disconnnected servers
func (m *Master) serverCheck() error {
	// detect dead servers
	addrs := m.csm.DetectDeadServers()
	for _, v := range addrs {
		log.Warningf("remove server %v", v)
		handles, err := m.csm.RemoveServer(v)
		if err != nil {
			return err
		}
		err = m.cm.RemoveChunks(handles, v)
		if err != nil {
			return err
		}
	}

	// add replicas for need request
	handles := m.cm.GetNeedlist()
	if handles != nil {
        log.Info("Master Need ", handles)
		m.cm.Lock()
		for i := 0; i < len(handles); i++ {
			ck := m.cm.chunk[handles[i]]

			if ck.expire.Before(time.Now()) {
				ck.Lock() // don't grant lease during copy
				err := m.reReplication(handles[i])
				log.Info(err)
				ck.Unlock()
			}
		}
		m.cm.Unlock()
	}
	return nil
}

// reReplication performs re-replication, ck should be locked in top caller
// new lease will not be granted during copy
func (m *Master) reReplication(handle gfs.ChunkHandle) error {
	// chunk are locked, so master will not grant lease during copy time
	from, to, err := m.csm.ChooseReReplication(handle)
	if err != nil {
		return err
	}
	log.Warningf("allocate new chunk %v from %v to %v", handle, from, to)

	var cr gfs.CreateChunkReply
	err = util.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{handle}, &cr)
	if err != nil {
		return err
	}

	var sr gfs.SendCopyReply
	err = util.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{handle, to}, &sr)
	if err != nil {
		return err
	}

	m.cm.RegisterReplica(handle, to, false)
	m.csm.AddChunk([]gfs.ServerAddress{to}, handle)
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
    isFirst := m.csm.Heartbeat(args.Address)

	for _, handle := range args.LeaseExtensions {
        continue
        // ATTENTION !! dead lock
		m.cm.ExtendLease(handle, args.Address)
	}

    if isFirst { // if is first heartbeat, let chunkserver report itself
        var r gfs.ReportSelfReply
        err := util.Call(args.Address, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &r)
        if err != nil {
            return err
        }

        for _, v := range r.Chunks {
            m.cm.RLock()
            version := m.cm.chunk[v.Handle].version
            m.cm.RUnlock()

            if v.Version == version {
                log.Infof("Master receive chunk %v from %v", v.Handle, args.Address)
                m.cm.RegisterReplica(v.Handle, args.Address, true)
                m.csm.AddChunk([]gfs.ServerAddress{args.Address}, v.Handle)
            } else {
                log.Infof("Master discard %v", v.Handle)
            }
        }
    }
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	lease, err := m.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}
	reply.Primary = lease.Primary
	reply.Expire = lease.Expire
	reply.Secondaries = lease.Secondaries
	return nil
}

// RPCExtendLease extends the lease of chunk if the lessee is nobody or requester.
func (m *Master) RPCExtendLease(args gfs.ExtendLeaseArg, reply *gfs.ExtendLeaseReply) error {
	//t, err := m.cm.ExtendLease(args.Handle, args.Address)
	//if err != nil { return err }
	//reply.Expire = *t
	return nil
}

// RPCGetReplicas is called by client to find all chunkserver that holds the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	servers, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	for _, v := range servers {
		reply.Locations = append(reply.Locations, v)
	}
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	err := m.nm.Create(args.Path)
	return err
}

// RPCDelete is called by client to delete a file
func (m *Master) RPCDeleteFile(args gfs.DeleteFileArg, reply *gfs.DeleteFileReply) error {
	err := m.nm.Delete(args.Path)
	return err
}

// RPCRename is called by client to rename a file
func (m *Master) RPCRenameFile(args gfs.RenameFileArg, reply *gfs.RenameFileReply) error {
	err := m.nm.Rename(args.Source, args.Target)
	return err
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	err := m.nm.Mkdir(args.Path)
	return err
}

// RPCList is called by client to list all files in specific directory
func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	var err error
	reply.Files, err = m.nm.List(args.Path)
	return err
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	ps, cwd, err := m.nm.lockParents(args.Path, false)
	defer m.nm.unlockParents(ps)
	if err != nil {
		return err
	}

	file, ok := cwd.children[ps[len(ps)-1]]
	if !ok {
		return fmt.Errorf("File %v does not exist", args.Path)
	}
	file.Lock()
	defer file.Unlock()

	reply.IsDir = file.isDir
	reply.Length = file.length
	reply.Chunks = file.chunks
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	ps, cwd, err := m.nm.lockParents(args.Path, false)
	defer m.nm.unlockParents(ps)
	if err != nil {
		return err
	}

	// append new chunks
	file, ok := cwd.children[ps[len(ps)-1]]
	if !ok {
		return fmt.Errorf("File %v does not exist", args.Path)
	}
	file.Lock()
	defer file.Unlock()

	if int(args.Index) == int(file.chunks) {
		file.chunks++

		addrs, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
		if err != nil {
			return err
		}

		reply.Handle, addrs, err = m.cm.CreateChunk(args.Path, addrs)
		if err != nil {
			// WARNING
			log.Warning("[ignored] An ignored error in RPCGetChunkHandle when create ", err, " in create chunk ", reply.Handle)
		}

		m.csm.AddChunk(addrs, reply.Handle)

		if len(addrs) < gfs.DefaultNumReplicas { // error in create chunk
			m.cm.Lock()
			m.cm.replicasNeedList = append(m.cm.replicasNeedList, reply.Handle)
			m.cm.Unlock()
		}
	} else {
		reply.Handle, err = m.cm.GetChunk(args.Path, args.Index)
	}

	return err
}
