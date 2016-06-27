package master

import (
	"time"

	"github.com/abcdabcd987/llgfs"
)

type HeartbeatArg struct {
	Address         llgfs.ServerAddress // chunkserver address
	LeaseExtensions []llgfs.ChunkHandle // leases to be extended
}

type HeartbeatReply struct{}

// Heartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (m *Master) Heartbeat(args HeartbeatArg, reply *HeartbeatReply) error {
	m.csm.Heartbeat(args.Address)
	for _, chunk := range args.LeaseExtensions {
		m.cm.ExtendLease(chunk, args.Address)
	}
	return nil
}

type AddAllChunksArg struct {
	Address llgfs.ServerAddress
	Chunks  []llgfs.ChunkHandle
}

type AddAllChunksReply struct{}

// AddAllChunks is called by chunkserver to adds all chunks from a chunkserver
func (m *Master) AddAllChunks(args AddAllChunksArg, reply *AddAllChunksReply) error {
	m.csm.AddChunks(args.Address, args.Chunks)
	for _, v := range args.Chunks {
		m.cm.AddReplica(v, args.Address)
	}
	return nil
}

type GetLesseeArg struct {
	Handle llgfs.ChunkHandle
}

type GetLesseeReply struct {
	Primary     llgfs.ServerAddress
	Expire      time.Time
	Secondaries []llgfs.ServerAddress
}

// GetLessee is called by client to find the chunkserver that holds the lease of
// a chunk (i.e. primary) and expire time of the lease. If no one has a lease,
// master grants one to a replica it chooses.
func (m *Master) GetLessee(args GetLesseeArg, reply *GetLesseeReply) error {
	l, err := m.cm.GetLessee(args.Handle)
	if err != nil {
		return err
	}
	reply.Primary = l.primary
	reply.Expire = l.expire
	reply.Secondaries = l.secondaries
	return nil
}

type ExtendLeaseArg struct {
	Handle  llgfs.ChunkHandle
	Address llgfs.ServerAddress
}

type ExtendLeaseReply struct {
	Expire time.Time
}

// ExtendLease extends the lease of chunk if the lessee is nobody or requester.
func (m *Master) ExtendLease(args ExtendLeaseArg, reply *ExtendLeaseReply) error {
	t, err := m.cm.ExtendLease(args.Handle, args.Address)
	if err != nil {
		return err
	}
	reply.Expire = *t
	return nil
}

type GetReplicasArg struct {
	Handle llgfs.ChunkHandle
}

type GetReplicasReply struct {
	Locations []llgfs.ServerAddress
}

// GetReplicas is called by client to find all chunkserver that holds the chunk.
func (m *Master) GetReplicas(args GetReplicasArg, reply *GetReplicasReply) error {
	s, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	var l []llgfs.ServerAddress
	for _, v := range s.GetAll() {
		l = append(l, v.(llgfs.ServerAddress))
	}
	reply.Locations = l
	return nil
}

type GetChunkArg struct {
	Path          string
	Index         int64
	AddIfNotExist bool
}

type GetChunkReply struct {
	llgfs.ChunkInfo
}

func (m *Master) GetChunk(args GetChunkArg, reply *GetChunkReply) error {

}
