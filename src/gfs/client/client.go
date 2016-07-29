package client

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/abcdabcd987/llgfs/gfs"
	"github.com/abcdabcd987/llgfs/gfs/util"
)

// Client struct is the GFS client-side driver
type Client struct {
	master gfs.ServerAddress
}

// NewClient returns a new gfs client.
func NewClient(master gfs.ServerAddress) *Client {
	return &Client{
		master: master,
	}
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	var reply gfs.GetChunkHandleReply
	err := util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{path, index}, &reply)
	if err != nil {
		return 0, err
	}
	return reply.Handle, nil
}

// WriteChunk writes data to the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	if len(data)+int(offset) > gfs.MaxChunkSize {
		return fmt.Errorf("len(data)+offset = %v > max chunk size %v", len(data)+int(offset), gfs.MaxChunkSize)
	}
	var l gfs.GetPrimaryAndSecondariesReply
	err := util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{handle}, &l)
	if err != nil {
		return err
	}

	var d gfs.PushDataAndForwardReply
	err = util.Call(l.Primary, "ChunkServer.RPCPushDataAndForward", gfs.PushDataAndForwardArg{handle, data, l.Secondaries}, &d)
	if err != nil {
		return err
	}

	wcargs := gfs.WriteChunkArg{d.DataID, offset, l.Secondaries}
	err = util.Call(l.Primary, "ChunkServer.RPCWriteChunk", wcargs, &gfs.WriteChunkReply{})
	return err
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// <code>len(data)</code> should be within 1/4 chunk size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	if len(data) > gfs.MaxAppendSize {
		return 0, fmt.Errorf("len(data) = %v > max append size %v", len(data), gfs.MaxAppendSize)
	}
	var l gfs.GetPrimaryAndSecondariesReply
	err = util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{handle}, &l)
	if err != nil {
		return
	}

    log.Infof("Client : push data to primary")
	var d gfs.PushDataAndForwardReply
	err = util.Call(l.Primary, "ChunkServer.RPCPushDataAndForward", gfs.PushDataAndForwardArg{handle, data, l.Secondaries}, &d)
	if err != nil {
		return
	}

    log.Infof("Client : send append request to primary. data : %v", d.DataID)
	var a gfs.AppendChunkReply
	acargs := gfs.AppendChunkArg{d.DataID, l.Secondaries}
	err = util.Call(l.Primary, "ChunkServer.RPCAppendChunk", acargs, &a)
	offset = a.Offset
	return
}

func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
    return nil
}

func (c *Client) Create(path gfs.Path) error {
    var reply struct {}
    err := util.Call(c.master, "Master.RPCCreateFile", gfs.CreateFileArg{path}, &reply)
    if err != nil { return err }
    return nil
}

func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	if len(data) > gfs.MaxAppendSize {
		return 0, fmt.Errorf("len(data) = %v > max append size %v", len(data), gfs.MaxAppendSize)
	}

    var h gfs.GetChunkHandleReply

    err = util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{path, -1}, &h)
    if err != nil { return }

    var chunkOffset gfs.Offset
    chunkOffset, err = c.AppendChunk(h.Handle, data)
    offset = chunkOffset
    return
}

    /*
    var f gfs.GetFileInfoReply

    err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{path}, &f)
    if err != nil { return }
    */
