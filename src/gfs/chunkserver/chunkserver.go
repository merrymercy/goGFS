package chunkserver

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	//"math/rand"
	"encoding/gob"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
	//"strings"

	"gfs"
	"gfs/util"
)

// ChunkServer struct
type ChunkServer struct {
	lock     sync.RWMutex
	address  gfs.ServerAddress // chunkserver address
	master   gfs.ServerAddress // master address
	rootDir  string            // path to data storage
	l        net.Listener
	shutdown chan struct{}

	dl                     *downloadBuffer                // expiring download buffer
	chunk                  map[gfs.ChunkHandle]*chunkInfo // chunk information
	dead                   bool                           // set to ture if server is shuntdown
	pendingLeaseExtensions *util.ArraySet                 // pending lease extension
	garbage                []gfs.ChunkHandle              // garbages
}

type Mutation struct {
	mtype  gfs.MutationType
	data   []byte
	offset gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	length    gfs.Offset
	version   gfs.ChunkVersion // version number of the chunk in disk
	checksum  gfs.Checksum
	mutations map[gfs.ChunkVersion]*Mutation // mutation buffer
	abandoned bool                           // unrecoverable error
}

const (
	MetaFileName = "gfs-server.meta"
	FilePerm     = 0755
)

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, rootDir string) *ChunkServer {
	cs := &ChunkServer{
		address:  addr,
		shutdown: make(chan struct{}),
		master:   masterAddr,
		rootDir:  rootDir,
		dl:       newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("chunkserver listen error:", e)
	}
	cs.l = l

	// Mkdir
	_, err := os.Stat(rootDir)
	if err != nil { // not exist
		err := os.Mkdir(rootDir, FilePerm)
		if err != nil {
			log.Fatal("error in mkdir ", err)
		}
	}

	err = cs.loadMeta()
	if err != nil {
		log.Warning("Error in load metadata: ", err)
	}

	// RPC Handler
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				if !cs.dead {
					log.Fatal("chunkserver accept error: ", err)
				}
			}
		}
	}()

	// Background Activity
	// heartbeat, store persistent meta, garbage collection ...
	go func() {
		heartbeatTicker := time.Tick(gfs.HeartbeatInterval)
		storeTicker := time.Tick(gfs.ServerStoreInterval)
		garbageTicker := time.Tick(gfs.GarbageCollectionInt)
		quickStart := make(chan bool, 1) // send first heartbeat right away..
		quickStart <- true
		for {
			var err error
			var branch string
			select {
			case <-cs.shutdown:
				return
			case <-quickStart:
				branch = "heartbeat"
				err = cs.heartbeat()
			case <-heartbeatTicker:
				branch = "heartbeat"
				err = cs.heartbeat()
			case <-storeTicker:
				branch = "storemeta"
				err = cs.storeMeta()
			case <-garbageTicker:
				branch = "garbagecollecton"
				err = cs.garbageCollection()
			}

			if err != nil {
				log.Errorf("%v background(%v) error %v", cs.address, branch, err)
			}
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, rootDir, masterAddr)

	return cs
}

// heartbeat calls master regularly to report chunkserver's status
func (cs *ChunkServer) heartbeat() error {
	pe := cs.pendingLeaseExtensions.GetAllAndClear()
	le := make([]gfs.ChunkHandle, len(pe))
	for i, v := range pe {
		le[i] = v.(gfs.ChunkHandle)
	}
	args := &gfs.HeartbeatArg{
		Address:         cs.address,
		LeaseExtensions: le,
	}
	var r gfs.HeartbeatReply
	err := util.Call(cs.master, "Master.RPCHeartbeat", args, &r)
	if err != nil {
		return err
	}

	cs.garbage = append(cs.garbage, r.Garbage...)
	return err
}

// garbage collection  Note: no lock are needed, since the background activities are single thread
func (cs *ChunkServer) garbageCollection() error {
	for _, v := range cs.garbage {
		cs.deleteChunk(v)
	}

	cs.garbage = make([]gfs.ChunkHandle, 0)
	return nil
}

// RPCReportSelf reports all chunks the server holds
func (cs *ChunkServer) RPCReportSelf(args gfs.ReportSelfArg, reply *gfs.ReportSelfReply) error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	log.Debug(cs.address, " report collect start")
	var ret []gfs.PersistentChunkInfo
	for handle, ck := range cs.chunk {
		//log.Info(cs.address, " report ", handle)
		ret = append(ret, gfs.PersistentChunkInfo{
			Handle:   handle,
			Version:  ck.version,
			Length:   ck.length,
			Checksum: ck.checksum,
		})
	}
	reply.Chunks = ret
	log.Debug(cs.address, " report collect end")

	return nil
}

// loadMeta loads metadata from disk
func (cs *ChunkServer) loadMeta() error {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	filename := path.Join(cs.rootDir, MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []gfs.PersistentChunkInfo
	dec := gob.NewDecoder(file)
	err = dec.Decode(&metas)
	if err != nil {
		return err
	}

	log.Infof("Server %v : load metadata len: %v", cs.address, len(metas))

	// load into memory
	for _, ck := range metas {
		//log.Infof("Server %v restore %v version: %v length: %v", cs.address, ck.Handle, ck.Version, ck.Length)
		cs.chunk[ck.Handle] = &chunkInfo{
			length:  ck.Length,
			version: ck.Version,
		}
	}

	return nil
}

// storeMeta stores metadate to disk
func (cs *ChunkServer) storeMeta() error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	filename := path.Join(cs.rootDir, MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []gfs.PersistentChunkInfo
	for handle, ck := range cs.chunk {
		metas = append(metas, gfs.PersistentChunkInfo{
			Handle: handle, Length: ck.length, Version: ck.version,
		})
	}

	log.Infof("Server %v : store metadata len: %v", cs.address, len(metas))
	enc := gob.NewEncoder(file)
	err = enc.Encode(metas)

	return err
}

// Shutdown shuts the chunkserver down
//func (cs *ChunkServer) Shutdown(args gfs.Nouse, reply *gfs.Nouse) error {
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warning(cs.address, " Shutdown")
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
	err := cs.storeMeta()
	if err != nil {
		log.Warning("error in store metadeta: ", err)
	}
}

// RPCCheckVersion is called by master to check version ande detect stale chunk
func (cs *ChunkServer) RPCCheckVersion(args gfs.CheckVersionArg, reply *gfs.CheckVersionReply) error {
	cs.lock.RLock()
	ck, ok := cs.chunk[args.Handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", args.Handle)
	}

	ck.Lock()
	defer ck.Unlock()

	if ck.version+gfs.ChunkVersion(1) == args.Version {
		ck.version++
		reply.Stale = false
	} else {
		log.Warningf("%v : stale chunk %v", cs.address, args.Handle)
		ck.abandoned = true
		reply.Stale = true
	}
	return nil
}

// RPCForwardData is called by client or another replica who sends data to the current memory buffer.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	//log.Warning(cs.address, " data 1 ", args.DataID)
	if _, ok := cs.dl.Get(args.DataID); ok {
		return fmt.Errorf("Data %v already exists", args.DataID)
	}

	//log.Infof("Server %v : get data %v", cs.address, args.DataID)
	//log.Warning(cs.address, "data 2 ", args.DataID)
	cs.dl.Set(args.DataID, args.Data)
	//log.Warning(cs.address, "data 3 ", args.DataID)

	if len(args.ChainOrder) > 0 {
		next := args.ChainOrder[0]
		args.ChainOrder = args.ChainOrder[1:]
		err := util.Call(next, "ChunkServer.RPCForwardData", args, reply)
		return err
	}
	//log.Warning(cs.address, "data 4 ", args.DataID)

	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	log.Infof("Server %v : create chunk %v", cs.address, args.Handle)

	if _, ok := cs.chunk[args.Handle]; ok {
		log.Warning("[ignored] recreate a chunk in RPCCreateChunk")
		return nil // TODO : error handle
		//return fmt.Errorf("Chunk %v already exists", args.Handle)
	}

	cs.chunk[args.Handle] = &chunkInfo{
		length: 0,
	}
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", args.Handle))
	_, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	// read from disk
	var err error
	reply.Data = make([]byte, args.Length)
	ck.RLock()
	reply.Length, err = cs.readChunk(handle, args.Offset, reply.Data)
	ck.RUnlock()
	if err == io.EOF {
		reply.ErrorCode = gfs.ReadEOF
		return nil
	}

	if err != nil {
		return err
	}
	return nil
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	newLen := args.Offset + gfs.Offset(len(data))
	if newLen > gfs.MaxChunkSize {
		return fmt.Errorf("writeChunk new length is too large. Size %v > MaxSize %v", len(data), gfs.MaxChunkSize)
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	if err = func() error {
		ck.Lock()
		defer ck.Unlock()
		mutation := &Mutation{gfs.MutationWrite, data, args.Offset}

		// apply to local
		wait := make(chan error, 1)
		go func() {
			wait <- cs.doMutation(handle, mutation)
		}()

		// call secondaries
		callArgs := gfs.ApplyMutationArg{gfs.MutationWrite, args.DataID, args.Offset}
		err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", callArgs)
		if err != nil {
			return err
		}

		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	// extend lease
	//cs.pendingLeaseExtensions.Add(handle)

	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within 1/4 chunk size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	if len(data) > gfs.MaxAppendSize {
		return fmt.Errorf("Append data size %v excceeds max append size %v", len(data), gfs.MaxAppendSize)
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	var mtype gfs.MutationType

	if err = func() error {
		ck.Lock()
		defer ck.Unlock()
		newLen := ck.length + gfs.Offset(len(data))
		offset := ck.length
		if newLen > gfs.MaxChunkSize {
			mtype = gfs.MutationPad
			ck.length = gfs.MaxChunkSize
			reply.ErrorCode = gfs.AppendExceedChunkSize
		} else {
			mtype = gfs.MutationAppend
			ck.length = newLen
		}
		reply.Offset = offset

		mutation := &Mutation{mtype, data, offset}

		//log.Infof("Primary %v : append chunk %v version %v", cs.address, args.DataID.Handle, version)

		// apply to local
		wait := make(chan error, 1)
		go func() {
			wait <- cs.doMutation(handle, mutation)
		}()

		// call secondaries
		callArgs := gfs.ApplyMutationArg{mtype, args.DataID, offset}
		err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", callArgs)
		if err != nil {
			return err
		}

		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	// extend lease
	//cs.pendingLeaseExtensions.Add(handle)

	return nil
}

// RPCApplyWriteChunk is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	//log.Infof("Server %v : get mutation to chunk %v version %v", cs.address, handle, args.Version)

	mutation := &Mutation{args.Mtype, data, args.Offset}
	err = func() error {
		ck.Lock()
		defer ck.Unlock()
		err = cs.doMutation(handle, mutation)
		return err
	}()

	return err
}

// RPCSendCCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	ck.RLock()
	defer ck.RUnlock()

	log.Infof("Server %v : Send copy of %v to %v", cs.address, handle, args.Address)
	data := make([]byte, ck.length)
	_, err := cs.readChunk(handle, 0, data)
	if err != nil {
		return err
	}

	var r gfs.ApplyCopyReply
	err = util.Call(args.Address, "ChunkServer.RPCApplyCopy", gfs.ApplyCopyArg{handle, data, ck.version}, &r)
	if err != nil {
		return err
	}

	return nil
}

// RPCSendCCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	log.Infof("Server %v : Apply copy of %v", cs.address, handle)

	ck.version = args.Version
	err := cs.writeChunk(handle, args.Data, 0, true)
	if err != nil {
		return err
	}
	log.Infof("Server %v : Apply done", cs.address)
	return nil
}

// writeChunk writes data at offset to a chunk at disk
func (cs *ChunkServer) writeChunk(handle gfs.ChunkHandle, data []byte, offset gfs.Offset, lock bool) error {
	cs.lock.RLock()
	ck := cs.chunk[handle]
	cs.lock.RUnlock()

	// ck is already locked in top caller
	newLen := offset + gfs.Offset(len(data))
	if newLen > ck.length {
		ck.length = newLen
	}

	if newLen > gfs.MaxChunkSize {
		log.Fatal("new length > gfs.MaxChunkSize")
	}

	log.Infof("Server %v : write to chunk %v at %v len %v", cs.address, handle, offset, len(data))
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(offset))
	if err != nil {
		return err
	}

	return nil
}

// readChunk reads data at offset from a chunk at dist
func (cs *ChunkServer) readChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))

	f, err := os.Open(filename)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	log.Infof("Server %v : read chunk %v at %v len %v", cs.address, handle, offset, len(data))
	return f.ReadAt(data, int64(offset))
}

// deleteChunk deletes a chunk during garbage collection
func (cs *ChunkServer) deleteChunk(handle gfs.ChunkHandle) error {
	cs.lock.Lock()
	delete(cs.chunk, handle)
	cs.lock.Unlock()

	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	err := os.Remove(filename)
	return err
}

// apply mutations (write, append, pad) in chunk buffer in proper order according to version number
func (cs *ChunkServer) doMutation(handle gfs.ChunkHandle, m *Mutation) error {
	// already locked
	var lock bool
	if m.mtype == gfs.MutationAppend {
		lock = true
	} else {
		lock = false
	}

	var err error
	if m.mtype == gfs.MutationPad {
		data := []byte{0}
		err = cs.writeChunk(handle, data, gfs.MaxChunkSize-1, lock)
	} else {
		err = cs.writeChunk(handle, m.data, m.offset, lock)
	}

	if err != nil {
		cs.lock.RLock()
		ck := cs.chunk[handle]
		cs.lock.RUnlock()
		log.Warningf("%v abandon chunk %v", cs.address, handle)
		ck.abandoned = true
		return err
	}

	return nil
}

// padChunk pads a chunk to max chunk size.
// <code>cs.chunk[handle]</code> should be locked in advance
func (cs *ChunkServer) padChunk(handle gfs.ChunkHandle, version gfs.ChunkVersion) error {
	log.Fatal("ChunkServer.padChunk is deserted!")
	//ck := cs.chunk[handle]
	//ck.version = version
	//ck.length = gfs.MaxChunkSize
	return nil
}

// =================== DEBUG TOOLS ===================
func getContents(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close() // f.Close will run when we're finished.

	var result []byte
	buf := make([]byte, 100)
	for {
		n, err := f.Read(buf[0:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err // f will be closed if we return here.
		}
		result = append(result, buf[0:n]...) // append is discussed later.
	}
	return string(result), nil // f will be closed if we return here.
}

func (cs *ChunkServer) PrintSelf(no1 gfs.Nouse, no2 *gfs.Nouse) error {
	cs.lock.RLock()
	cs.lock.RUnlock()
	log.Info("============ ", cs.address, " ============")
	if cs.dead {
		log.Warning("DEAD")
	} else {
		for h, v := range cs.chunk {
			filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", h))
			log.Infof("chunk %v : version %v", h, v.version)
			str, _ := getContents(filename)
			log.Info(str)
		}
	}
	return nil
}
