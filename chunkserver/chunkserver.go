package chunkserver

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path"
	"time"

	"github.com/abcdabcd987/llgfs"
	"github.com/abcdabcd987/llgfs/master"
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
	chunks            map[llgfs.ChunkHandle]chunkInfo // chunk information
}

type chunkInfo struct {
	length int64
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
		args := &master.HeartbeatArg{
			Address:         address,
			LeaseExtensions: le,
		}
		if err := util.Call(cs.master, "Master.Heartbeat", args, nil); err != nil {
			log.Fatal("heartbeat rpc error", err)
		}

		time.Sleep(llgfs.HeartbeatInterval)
	}()

	return cs
}

// writeChunk writes data at offset to a chunk at disk
func (cs *ChunkServer) writeChunk(handle llgfs.ChunkHandle, data []byte, offset int64) error {
	filename := path.Join(cs.serverRoot, fmt.Sprintf("chunks/%v.chunk", handle))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteAt(data, offset)
	return err
}

// applyToSecondaries applies write request to secondary replicas
func (cs *ChunkServer) applyToSecondaries(args *WriteSecondaryArg, loc []llgfs.ServerAddress) error {
	var ch chan error
	var err error
	apply := func(args *WriteSecondaryArg, addr llgfs.ServerAddress) {
		ch <- util.Call(addr, "ChunkServer.WriteSecondary", args, nil)
	}
	for _, addr := range loc {
		go apply(args, addr)
	}
	for _ = range loc {
		if e := <-ch; e != nil {
			err = e
		}
	}
	return err
}
