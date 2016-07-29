package chunkserver

import (
    "fmt"
	log "github.com/Sirupsen/logrus"
	//"math/rand"
	"net"
	"net/rpc"
	"os"
    "io"
	"path"
	"sync"
	"time"

	"github.com/abcdabcd987/llgfs/gfs"
	"github.com/abcdabcd987/llgfs/gfs/util"
)

// ChunkServer struct
type ChunkServer struct {
	address    gfs.ServerAddress // chunkserver address
	master     gfs.ServerAddress // master address
	serverRoot string            // path to data storage
	l          net.Listener
	shutdown   chan struct {}

	dl                *downloadBuffer               // expiring download buffer
	pendingExtensions *util.ArraySet                // pending lease extension
	chunk             map[gfs.ChunkHandle]*chunkInfo // chunk information
}

type Mutation struct {
    mtype   gfs.MutationType
    version gfs.ChunkVersion
    data    []byte
    offset  gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	length  gfs.Offset
    version gfs.ChunkVersion
    mutations []Mutation
}

func (ck *chunkInfo) nextVersion() gfs.ChunkVersion {
    return ck.version + 1
}

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:           addr,
        shutdown:          make(chan struct{}),
		master:            masterAddr,
		serverRoot:        serverRoot,
		dl:                newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingExtensions: new(util.ArraySet),
        chunk:             make(map[gfs.ChunkHandle]*chunkInfo),
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	cs.l = l

    // Mkdir
    err := os.Mkdir(serverRoot, 0744)
    if err != nil { log.Fatal("mkdir", err) }

	shutdown := make(chan struct{})
	// RPC Handler
	go func() {
	loop:
		for {
			select {
			case <-cs.shutdown:
                close(shutdown)
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
	loop:
		for {
			select {
			case <-shutdown:
				break loop
			default:
			}
			pe := cs.pendingExtensions.GetAllAndClear()
			le := make([]gfs.ChunkHandle, len(pe))
			for i, v := range pe {
				le[i] = v.(gfs.ChunkHandle)
			}
			args := &gfs.HeartbeatArg{
				Address:         addr,
				LeaseExtensions: le,
			}
			if err := util.Call(cs.master, "Master.RPCHeartbeat", args, nil); err != nil {
				log.Fatal("heartbeat rpc error", err)
				log.Exit(1)
			}

			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
    close(cs.shutdown)
    //cs.l.Close()
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer and forward to all other replicas.
// Returns a DataID which represents the index in the memory buffer.
func (cs *ChunkServer) RPCPushDataAndForward(args gfs.PushDataAndForwardArg, reply *gfs.PushDataAndForwardReply) error {
	if len(args.Data) > gfs.MaxChunkSize {
		return fmt.Errorf("Data is too large. Size %v > MaxSize %v", len(args.Data), gfs.MaxChunkSize)
	}

    id := cs.dl.New(args.Handle)
    cs.dl.Set(id, args.Data)
    log.Infof("Server %v : get data %v (primary)", cs.address, id)

    err := util.CallAll(args.ForwardTo, "ChunkServer.RPCForwardData", gfs.ForwardDataArg{id, args.Data})

    reply.DataID = id
    return err
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
// TODO: This should be replaced by a chain forwarding.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
    if _, ok := cs.dl.Get(args.DataID); ok {
        return fmt.Errorf("Data %v already exists", args.DataID)
    }

    log.Infof("Server %v : get data %v", cs.address, args.DataID)
    cs.dl.Set(args.DataID, args.Data)
    return nil
}

// deleteDownloadedData returns the corresponding data and delete it from the buffer.
func (cs *ChunkServer) deleteDownloadedData(id gfs.DataBufferID) ([]byte, error) {
	data, ok := cs.dl.Get(id)
	if !ok {
		return nil, fmt.Errorf("DataID %v not found in download buffer.", id)
	}
	cs.dl.Delete(id)

	return data, nil
}

// writeChunk writes data at offset to a chunk at disk
func (cs *ChunkServer) writeChunk(handle gfs.ChunkHandle, version gfs.ChunkVersion, data []byte, offset gfs.Offset, lock bool) error {
    ck := cs.chunk[handle]

    /* if lock {
        ck.Lock()
        defer ck.Unlock()
    } else {
        ck.RLock()
        defer ck.RUnlock()
    }*/

    log.Infof("Server %v : write to chunk %v version %v", cs.address, handle, version)
    filename := path.Join(cs.serverRoot, fmt.Sprintf("chunk%v.chk", handle))
    file, err := os.OpenFile(filename, os.O_WRONLY | os.O_CREATE, 0744)
    if err != nil { return err }
    defer file.Close()

     _, err = file.WriteAt(data, int64(offset));
    if err != nil { return err }

    newLen := offset + gfs.Offset(len(data))
    if newLen > ck.length {
        ck.length = newLen
    }
    ck.version = version

    return nil
}

// TODO : optimum
func (cs *ChunkServer) doMutation(handle gfs.ChunkHandle) error {
    //log.Warning(handle, ck, "version: ", ck.version)
    //log.Warningf("mutation loop %v begin", cs.address)
	ck := cs.chunk[handle]
    for {
        again := false
        for i, v := range ck.mutations {
            if v.version <= ck.version {

                //return fmt.Errorf("Chunk %v missed a mutation", handle)
            } else if v.version == ck.version + 1 {
                var lock bool
                if v.mtype == gfs.MutationAppend {
                    lock = true
                } else {
                    lock = false
                }
                _ = lock

                var err error
                if v.mtype == gfs.MutationPad {
                    err = cs.padChunk(handle, v.version)
                } else {
                    err = cs.writeChunk(handle, v.version, v.data, v.offset, lock)
                }
                if err != nil { return err }
                ck.mutations = append(ck.mutations[:i], ck.mutations[i+1:]...)

                again = true
                break;
            }
        }
        if !again { break }
    }
    //log.Warningf("mutation loop %v end", cs.address)
    return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
    log.Infof("%v create chunk %v", cs.address, args.Handle)

    if _, ok := cs.chunk[args.Handle]; ok {
        return fmt.Errorf("Chunk %v already exists", args.Handle)
    }

    cs.chunk[args.Handle] = &chunkInfo{length: 0}
    //_, err := os.Create(fmt.Sprintf("%s/chunk%d.chk", cs.serverRoot, args.Handle))
    return nil
}

// RPCWriteChunk applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
    data, err := cs.deleteDownloadedData(args.DataID)
    if err != nil { return err }

    handle := args.DataID.Handle
    ck, ok := cs.chunk[handle]

    ck.Lock()
    defer ck.Unlock()

    if !ok { return fmt.Errorf("Cannot find chunk %v", handle) }

    newLen := args.Offset + gfs.Offset(len(data))
    if newLen > gfs.MaxChunkSize {
		return fmt.Errorf("writeChunk new length is too large. Size %v > MaxSize %v", len(data), gfs.MaxChunkSize)
    }

    // assign a new version
    version := ck.nextVersion()

    // write local
    log.Warningf("Server %v write %v", cs.address, args.DataID)
    err = cs.writeChunk(handle, version, data, args.Offset, false);
    if err != nil { return err }

    // call secondaries
    callArgs := gfs.ApplyMutationArg{gfs.MutationWrite, version, args.DataID, args.Offset}
    err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", callArgs);
    if err != nil { return err }

    return nil
}


// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within 1/4 chunk size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	data, err := cs.deleteDownloadedData(args.DataID)
	if err != nil { return err }

	if len(data) > gfs.MaxAppendSize {
		return fmt.Errorf("Append data size %v excceeds max append size %v", len(data), gfs.MaxAppendSize)
	}

    handle := args.DataID.Handle
	ck, ok := cs.chunk[handle]
	if !ok { return fmt.Errorf("cannot find chunk %v", handle) }


    var (
        mtype gfs.MutationType
    )

    ck.Lock()
	newLen := ck.length + gfs.Offset(len(data))
	offset := ck.length
	if newLen > gfs.MaxChunkSize {
        mtype = gfs.MutationPad
        ck.length = gfs.MaxChunkSize
        reply.ErrorCode  = gfs.AppendExceedChunkSize
    } else {
        mtype = gfs.MutationAppend
        ck.length = newLen
    }
	reply.Offset = offset

    version := ck.nextVersion()
    ck.mutations = append(ck.mutations, Mutation{mtype, version, data, offset})
    ck.Unlock()

    log.Infof("Primary %v : append chunk %v version %v", cs.address, args.DataID.Handle, version)

    ck.Lock()
    err = cs.doMutation(handle)
    if err != nil { return err }
    ck.Unlock()

    // call secondaries
    callArgs := gfs.ApplyMutationArg{mtype, version, args.DataID, offset}
    err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", callArgs);
    if err != nil { return err }

    return nil
}

// RPCApplyWriteChunk is called by primary to apply chunk write.
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
    data, err := cs.deleteDownloadedData(args.DataID)
    if err != nil { return err }

    handle := args.DataID.Handle
    ck, ok := cs.chunk[handle]
    if !ok { return fmt.Errorf("Cannot find chunk %v", handle) }


    mutation := Mutation{args.Mtype, args.Version, data, args.Offset}
    ck.Lock()
    defer ck.Unlock()
    ck.mutations = append(ck.mutations, mutation)

    log.Infof("Server %v : get mutation to chunk %v version %v", cs.address, handle, args.Version)

    err = cs.doMutation(handle)
    return err
}


// padChunk pads a chunk to max chunk size.
// <code>cs.chunk[handle]</code> should be locked in advance
func (cs *ChunkServer) padChunk(handle gfs.ChunkHandle, version gfs.ChunkVersion) error {
    ck := cs.chunk[handle]
    ck.length = gfs.MaxChunkSize
    ck.version = version

    return nil
}

// RPCPadChunk is called by primary and it should pad the chunk to max size.
func (cs *ChunkServer) RPCPadChunk(args gfs.PadChunkArg, reply *gfs.PadChunkReply) error {
    return nil
}

func getContents(filename string) (string, error) {
    f, err := os.Open(filename)
    if err != nil {
        return "", err
    }
    defer f.Close()  // f.Close will run when we're finished.

    var result []byte
    buf := make([]byte, 100)
    for {
        n, err := f.Read(buf[0:])
        if err != nil {
            if err == io.EOF {
                break
            }
            return "", err  // f will be closed if we return here.
        }
        result = append(result, buf[0:n]...) // append is discussed later.
    }
    return string(result), nil // f will be closed if we return here.
}

func (cs *ChunkServer) PrintSelf() error {
    log.Info("============ ", cs.address, " ============")
    for h, _ := range cs.chunk {
        filename := path.Join(cs.serverRoot, fmt.Sprintf("chunk%v.chk", h))
        log.Infof("chunk %v :", h)
        str, _ := getContents(filename)
        log.Info(str)
    }

    return nil
}
