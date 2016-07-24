package master

import (
	log "github.com/Sirupsen/logrus"
	"github.com/abcdabcd987/llgfs/gfs"
	"github.com/abcdabcd987/llgfs/gfs/util"
	"net"
	"net/rpc"
)

// Master Server struct
type Master struct {
	address  gfs.ServerAddress // master server address
	l        net.Listener
	shutdown chan bool

	cm  *chunkManager
	csm *chunkServerManager
}

// NewAndServe starts a master and return the pointer to it.
func NewAndServe(address gfs.ServerAddress) *Master {
	m := &Master{
		address: address,
		cm:      new(chunkManager),
		csm:     new(chunkServerManager),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

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

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	m.csm.Heartbeat(args.Address)
	for _, chunk := range args.LeaseExtensions {
		m.cm.ExtendLease(chunk, args.Address)
	}
	return nil
}

// // RPCAddAllChunks is called by chunkserver to adds all chunks from a chunkserver
// func (m *Master) RPCAddAllChunks(args AddAllChunksArg, reply *AddAllChunksReply) error {
// 	m.csm.AddChunks(args.Address, args.Chunks)
// 	for _, v := range args.Chunks {
// 		m.cm.AddReplica(v, args.Address)
// 	}
// 	return nil
// }

// type AddAllChunksArg struct {
// 	Address gfs.ServerAddress
// 	Chunks  []gfs.ChunkHandle
// }

// type AddAllChunksReply struct{}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	l, e := m.cm.GetLeaseHolder(args.Handle)
	if e != nil {
		return e
	}
	reply.Expire = l.expire
	reply.Primary = l.primary
	reply.Secondaries = l.secondaries
	return nil
}

// RPCExtendLease extends the lease of chunk if the lessee is nobody or requester.
func (m *Master) RPCExtendLease(args gfs.ExtendLeaseArg, reply *gfs.ExtendLeaseReply) error {
	t, err := m.cm.ExtendLease(args.Handle, args.Address)
	if err != nil {
		return err
	}
	reply.Expire = *t
	return nil
}

// RPCGetReplicas is called by client to find all chunkserver that holds the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	s, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	var l []gfs.ServerAddress
	for _, v := range s.GetAll() {
		l = append(l, v.(gfs.ServerAddress))
	}
	reply.Locations = l
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
// Otherwise, an error will occur.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	handle, err := m.cm.CreateChunk(args.Path, args.Index)
	if err != nil {
		return err
	}
	srvs, err := m.csm.Sample(gfs.DefaultNumReplicas)
	if err != nil {
		return err
	}
	err = util.CallAll(srvs, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{handle})
	return err
}
