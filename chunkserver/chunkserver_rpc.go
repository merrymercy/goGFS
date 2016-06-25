package chunkserver

import (
	"fmt"
	"math/rand"

	"github.com/abcdabcd987/llgfs"
)

type ReceiveDataArg struct {
	Data []byte
}

type ReceiveDataReply struct {
	Id llgfs.DataBufferId
}

// ReceiveData is called by client to receive data to be written from client.
func (cs *ChunkServer) ReceiveData(args *ReceiveDataArg, reply *ReceiveDataReply) error {
	id := llgfs.DataBufferId(rand.Int63())
	reply.Id = id
	cs.dl.Set(id, args.Data)
	return nil
}

type WriteArg struct {
	Handle      llgfs.ChunkHandle
	DataID      llgfs.DataBufferId
	Offset      int64
	Secondaries []llgfs.ServerAddress
}

type WriteReply struct{}

func (cs *ChunkServer) Write(args *WriteArg, reply *WriteReply) error {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("DataID %v not found in download buffer.", args.DataID)
	}

	// apply to local storage
	if err := cs.writeChunk(args.Handle, data, args.Offset); err != nil {
		return err
	}
	cs.dl.Delete(args.DataID)

	// apply to secondary
	wsargs := &WriteSecondaryArg{args.Handle, args.DataID, args.Offset}
	if err := cs.applyToSecondaries(wsargs, args.Secondaries); err != nil {
		return err
	}

	// extend lease
	cs.pendingLeaseExtensions.Add(args.Handle)

	return nil
}

type WriteSecondaryArg struct {
	Handle llgfs.ChunkHandle
	DataID llgfs.DataBufferId
	Offset int64
}

type WriteSecondaryReply struct{}

// WriteSecondary is called by primaray replica to write chunk data.
func (cs *ChunkServer) WriteSecondary(args *WriteSecondaryArg, reply *WriteSecondaryReply) error {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("DataID %v not found in download buffer.", args.DataID)
	}
	if err := cs.writeChunk(args.Handle, data, args.Offset); err != nil {
		return err
	}
	cs.dl.Delete(args.DataID)
	return nil
}
