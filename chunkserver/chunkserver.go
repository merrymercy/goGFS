package chunkserver

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"

	"github.com/abcdabcd987/llgfs"
	"github.com/abcdabcd987/llgfs/util"
)

// ChunkServer struct
type ChunkServer struct {
	address    llgfs.ServerAddress // chunkserver address
	master     llgfs.ServerAddress // master address
	serverRoot string              // path to data storage
	l          net.Listener
	shutdown   chan bool

	dl                *downloadBuffer                 // expiring download buffer
	pendingExtensions *util.ArraySet                  // pending lease extension
	chunk             map[llgfs.ChunkHandle]chunkInfo // chunk information
}

type chunkInfo struct {
	sync.RWMutex
	length llgfs.Offset
}

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr llgfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:           addr,
		master:            masterAddr,
		serverRoot:        serverRoot,
		dl:                new(downloadBuffer),
		pendingExtensions: new(util.ArraySet),
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	cs.l = l

	// RPC Handler
	go func() {
	loop:
		for {
			select {
			case <-cs.shutdown:
				break loop
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Fatal("accept error:", err)
			}
		}
	}()

	// Heartbeat
	go func() {
		pe := cs.pendingExtensions.GetAllAndClear()
		le := make([]llgfs.ChunkHandle, len(pe))
		for i, v := range pe {
			le[i] = v.(llgfs.ChunkHandle)
		}
		args := &llgfs.HeartbeatArg{
			Address:         addr,
			LeaseExtensions: le,
		}
		if err := util.Call(cs.master, "Master.Heartbeat", args, nil); err != nil {
			log.Fatal("heartbeat rpc error", err)
		}

		time.Sleep(llgfs.HeartbeatInterval)
	}()

	return cs
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer and forward to all other replicas.
// Returns a DataID which represents the index in the memory buffer.
func (cs *ChunkServer) RPCPushDataAndForward(args llgfs.PushDataAndForwardArg, reply *llgfs.PushDataAndForwardReply) error {
	cs.dl.Lock()
	defer cs.dl.Unlock()

	if len(args.Data) > llgfs.MaxChunkSize {
		return fmt.Errorf("Data is too large. Size %v > MaxSize %v", len(args.Data), llgfs.MaxChunkSize)
	}

	var id llgfs.DataBufferID
	for {
		id = llgfs.DataBufferID(rand.Int63())
		if _, ok := cs.dl.Get(id); !ok {
			break
		}
	}

	cs.dl.Set(id, args.Data)

	forwardArg := llgfs.ForwardDataArg{args.Data, id}
	err := util.CallAll(args.ForwardTo, "ChunkServer.RPCForwardData", forwardArg)
	return err
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
// TODO: This should be replaced by a chain forwarding.
func (cs *ChunkServer) RPCForwardData(args llgfs.ForwardDataArg, reply *llgfs.ForwardDataReply) error {
	cs.dl.Lock()
	defer cs.dl.Unlock()
	if _, ok := cs.dl.Get(args.DataID); ok {
		return fmt.Errorf("DataID %v already exists", args.DataID)
	}
	cs.dl.Set(args.DataID, args.Data)
	return nil
}

// deleteDownloadedData returns the corresponding data and delete it from the buffer.
func (cs *ChunkServer) deleteDownloadedData(id llgfs.DataBufferID) ([]byte, error) {
	cs.dl.Lock()
	defer cs.dl.Unlock()
	data, ok := cs.dl.Get(id)
	if !ok {
		return nil, fmt.Errorf("DataID %v not found in download buffer.", id)
	}
	cs.dl.Delete(id)
	return data, nil
}

// RPCWriteChunk applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args llgfs.WriteChunkArg, reply *llgfs.WriteChunkReply) error {
	data, err := cs.deleteDownloadedData(args.DataID)
	if err != nil {
		return err
	}

	// apply to local storage
	if err := cs.writeChunk(args.Handle, data, args.Offset, true); err != nil {
		return err
	}

	// apply to secondary
	awargs := llgfs.ApplyWriteChunkArg{args.Handle, args.Offset, args.DataID}
	if err := util.CallAll(args.Secondaries, "ChunkServer.ApplyWriteChunk", awargs); err != nil {
		return err
	}

	// extend lease
	//cs.pendingLeaseExtensions.Add(args.Handle)

	return nil
}

// RPCApplyWriteChunk is called by primary to apply chunk write.
func (cs *ChunkServer) RPCApplyWriteChunk(args llgfs.ApplyWriteChunkArg, reply *llgfs.ApplyWriteChunkReply) error {
	data, err := cs.deleteDownloadedData(args.DataID)
	if err != nil {
		return err
	}

	// apply to local storage
	if err := cs.writeChunk(args.Handle, data, args.Offset, true); err != nil {
		return err
	}
	return nil
}

// writeChunk writes data at offset to a chunk at disk
func (cs *ChunkServer) writeChunk(handle llgfs.ChunkHandle, data []byte, offset llgfs.Offset, lock bool) error {
	h, ok := cs.chunk[handle]
	if !ok {
		return fmt.Errorf("Chunk %v does not exist", handle)
	}
	if lock {
		h.Lock()
		defer h.Unlock()
	}

	newLength := offset + llgfs.Offset(len(data))
	if newLength > llgfs.MaxChunkSize {
		return fmt.Errorf("writeChunk new length %v exceed max chunk size %v", newLength, llgfs.MaxChunkSize)
	}

	filename := path.Join(cs.serverRoot, fmt.Sprintf("chunks/%v.chunk", handle))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = file.WriteAt(data, int64(offset)); err != nil {
		return err
	}
	if newLength > h.length {
		h.length = newLength
	}
	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args llgfs.CreateChunkArg, reply *llgfs.CreateChunkReply) error {
	if _, ok := cs.chunk[args.Handle]; ok {
		return fmt.Errorf("Chunk %v already exists", args.Handle)
	}
	cs.chunk[args.Handle] = chunkInfo{length: 0}
	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within 1/4 chunk size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args llgfs.AppendChunkArg, reply *llgfs.AppendChunkReply) error {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("DataID %v not found in download buffer.", args.DataID)
	}

	if len(data) > llgfs.MaxAppendSize {
		return fmt.Errorf("Append data size %v excceeds max append size %v", len(data), llgfs.MaxAppendSize)
	}

	h, ok := cs.chunk[args.Handle]
	if !ok {
		return fmt.Errorf("Chunk %v does not exist", args.Handle)
	}
	h.Lock()
	defer h.Unlock()
	newLength := h.length + llgfs.Offset(len(data))
	if newLength > llgfs.MaxChunkSize {
		// pad local
		cs.padChunk(args.Handle)

		// pad secondaries
		util.CallAll(args.Secondaries, "ChunkServer.PadChunk", llgfs.PadChunkArg{args.Handle})
		return fmt.Errorf("New chunk size %v excceeds max chunk size %v", newLength, llgfs.MaxChunkSize)
	}

	// write local
	offset := cs.chunk[args.Handle].length
	if err := cs.writeChunk(args.Handle, data, offset, false); err != nil {
		return err
	}

	// write secondary
	awargs := llgfs.ApplyWriteChunkArg{args.Handle, offset, args.DataID}
	if err := util.CallAll(args.Secondaries, "ChunkServer.ApplyWriteChunk", awargs); err != nil {
		return err
	}

	// clean up
	reply.Offset = offset
	h.length = newLength
	cs.dl.Delete(args.DataID)
	return nil
}

// padChunk pads a chunk to max chunk size.
// <code>cs.chunk[handle]</code> should be locked in advance
func (cs *ChunkServer) padChunk(handle llgfs.ChunkHandle) {
	h, _ := cs.chunk[handle]
	h.length = llgfs.MaxChunkSize
}

// RPCPadChunk is called by primary and it should pad the chunk to max size.
func (cs *ChunkServer) RPCPadChunk(args llgfs.PadChunkArg, reply *llgfs.PadChunkReply) error {
	h, ok := cs.chunk[args.Handle]
	if !ok {
		return fmt.Errorf("Chunk %v does not exist", args.Handle)
	}
	h.Lock()
	defer h.Unlock()
	cs.padChunk(args.Handle)
	return nil
}
