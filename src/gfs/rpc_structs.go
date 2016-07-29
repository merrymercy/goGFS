package gfs

import (
	"time"
)

//------ ChunkServer

type PushDataAndForwardArg struct {
	Data      []byte
	ForwardTo []ServerAddress
}

type PushDataAndForwardReply struct {
	DataID DataBufferID
}

type ForwardDataArg struct {
	Data   []byte
	DataID DataBufferID
}

type ForwardDataReply struct {
}

type WriteChunkArg struct {
	Handle      ChunkHandle
	Offset      Offset
	DataID      DataBufferID
	Secondaries []ServerAddress
}

type WriteChunkReply struct {
}

type ApplyWriteChunkArg struct {
	Handle ChunkHandle
	Offset Offset
	DataID DataBufferID
}

type ApplyWriteChunkReply struct {
}

type CreateChunkArg struct {
	Handle ChunkHandle
}

type CreateChunkReply struct {
}

type AppendChunkArg struct {
	Handle      ChunkHandle
	DataID      DataBufferID
	Secondaries []ServerAddress
}

type AppendChunkReply struct {
	Offset Offset
}

type PadChunkArg struct {
	Handle ChunkHandle
}

type PadChunkReply struct {
}

//------ Master

type HeartbeatArg struct {
	Address         ServerAddress // chunkserver address
	LeaseExtensions []ChunkHandle // leases to be extended
}

type HeartbeatReply struct{}

type GetPrimaryAndSecondariesArg struct {
	Handle ChunkHandle
}

type GetPrimaryAndSecondariesReply struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
}

type ExtendLeaseArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}

type ExtendLeaseReply struct {
	Expire time.Time
}

type GetReplicasArg struct {
	Handle ChunkHandle
}

type GetReplicasReply struct {
	Locations []ServerAddress
}

type GetChunkHandleArg struct {
	Path  Path
	Index ChunkIndex
}

type GetChunkHandleReply struct {
	Handle ChunkHandle
}
