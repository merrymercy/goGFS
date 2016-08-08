package gfs

import (
	"time"
)

/*
 *  ChunkServer
 */

// handshake
type CheckVersionArg struct {
	Handle  ChunkHandle
	Version ChunkVersion
}
type CheckVersionReply struct {
	Stale bool
}

// chunk IO
type ForwardDataArg struct {
	DataID     DataBufferID
	Data       []byte
	ChainOrder []ServerAddress
}
type ForwardDataReply struct {
	ErrorCode ErrorCode
}

type CreateChunkArg struct {
	Handle ChunkHandle
}
type CreateChunkReply struct {
	ErrorCode ErrorCode
}

type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      Offset
	Secondaries []ServerAddress
}
type WriteChunkReply struct {
	ErrorCode ErrorCode
}

type AppendChunkArg struct {
	DataID      DataBufferID
	Secondaries []ServerAddress
}
type AppendChunkReply struct {
	Offset    Offset
	ErrorCode ErrorCode
}

type ApplyMutationArg struct {
	Mtype  MutationType
	DataID DataBufferID
	Offset Offset
}
type ApplyMutationReply struct {
	ErrorCode ErrorCode
}

type PadChunkArg struct {
	Handle ChunkHandle
}
type PadChunkReply struct {
	ErrorCode ErrorCode
}

type ReadChunkArg struct {
	Handle ChunkHandle
	Offset Offset
	Length int
}
type ReadChunkReply struct {
	Data      []byte
	Length    int
	ErrorCode ErrorCode
}

// re-replication
type SendCopyArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type SendCopyReply struct {
	ErrorCode ErrorCode
}

type ApplyCopyArg struct {
	Handle  ChunkHandle
	Data    []byte
	Version ChunkVersion
}
type ApplyCopyReply struct {
	ErrorCode ErrorCode
}

// no use argument
type Nouse struct{}

/*
 *  Master
 */

// handshake
type HeartbeatArg struct {
	Address          ServerAddress // chunkserver address
	LeaseExtensions  []ChunkHandle // leases to be extended
	AbandondedChunks []ChunkHandle // unrecoverable chunks
}
type HeartbeatReply struct {
	Garbage []ChunkHandle
}

type ReportSelfArg struct {
}
type ReportSelfReply struct {
	Chunks []PersistentChunkInfo
}

// chunk info
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

type GetFileInfoArg struct {
	Path Path
}
type GetFileInfoReply struct {
	IsDir  bool
	Length int64
	Chunks int64
}

type GetChunkHandleArg struct {
	Path  Path
	Index ChunkIndex
}
type GetChunkHandleReply struct {
	Handle ChunkHandle
}

// namespace operation
type CreateFileArg struct {
	Path Path
}
type CreateFileReply struct{}

type DeleteFileArg struct {
	Path Path
}
type DeleteFileReply struct{}

type RenameFileArg struct {
	Source Path
	Target Path
}
type RenameFileReply struct{}

type MkdirArg struct {
	Path Path
}
type MkdirReply struct{}

type ListArg struct {
	Path Path
}
type ListReply struct {
	Files []PathInfo
}
