package master

import (
    //"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/abcdabcd987/llgfs/gfs"
	//"github.com/abcdabcd987/llgfs/gfs/util"
	"net"
	"net/rpc"
)

// Master Server struct
type Master struct {
	address       gfs.ServerAddress // master server address
	l             net.Listener
	shutdown      chan struct {}

    nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

// NewAndServe starts a master and return the pointer to it.
func NewAndServe(address gfs.ServerAddress) *Master {
	m := &Master{
		address:       address,
        shutdown:      make(chan struct {}),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

    m.InitMetadata()

	// RPC Handler
	go func() {
	loop:
		for {
			select {
			case <-m.shutdown:
				break loop
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Fatal("accept error:", err)
				log.Exit(1)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v", address)

	return m
}

// initiate meta data
func (m *Master) InitMetadata() {
    // new or read from old
    m.nm =  newNamespaceManager()
    m.cm  = newChunkManager()
    m.csm = newChunkServerManager()
    return
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	close(m.shutdown)
    //m.l.Close()
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
    m.csm.Heartbeat(args.Address)
    for _, handle := range args.LeaseExtensions {
        m.cm.ExtendLease(handle, args.Address)
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
    reply.Primary = lease.primary
    reply.Expire  = lease.expire
    reply.Secondaries = lease.secondaries
    return nil
}

// RPCExtendLease extends the lease of chunk if the lessee is nobody or requester.
func (m *Master) RPCExtendLease(args gfs.ExtendLeaseArg, reply *gfs.ExtendLeaseReply) error {
    t, err := m.cm.ExtendLease(args.Handle, args.Address)
    if err != nil { return err }
    reply.Expire = *t
    return nil
}

// RPCGetReplicas is called by client to find all chunkserver that holds the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
    servers, err := m.cm.GetReplicas(args.Handle)
    if err != nil { return err }

    for _, v := range servers.GetAll() {
        reply.Locations = append(reply.Locations, v.(gfs.ServerAddress))
    }

    return nil
}

// RPCCreateFile
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, replay *gfs.CreateFileReply) error {
    err := m.nm.Create(string(args.Path))
    return err
}

// RPCGetFileInfo
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
    ps, cwd, err := m.nm.lockParents(string(args.Path))
    defer m.nm.unlockParents(ps)
    if (err != nil) { return err }

    file := cwd.children[ps[len(ps)-1]]
    reply.IsDir  = file.isDir
    reply.Length = file.length
    reply.Chunks = file.chunks
    return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
    ps, cwd, err := m.nm.lockParents(string(args.Path))
    defer m.nm.unlockParents(ps)
    if err != nil { return err }

    // append new chunks
    file := cwd.children[ps[len(ps)-1]]
    if int(args.Index) == int(file.chunks) {
        file.chunks++
        addrs, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
        if err != nil { return err }
        reply.Handle, err = m.cm.CreateChunk(args.Path, addrs)
        for _, v := range addrs {
            m.cm.RegisterReplica(reply.Handle, v)
        }
        if err != nil { return err }
        err = m.csm.AddChunk(addrs, reply.Handle)
    } else {
        reply.Handle, err = m.cm.GetChunk(args.Path, args.Index)
    }

    return err
}
