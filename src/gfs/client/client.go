package client

import (
	"fmt"
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
	err = util.Call(l.Primary, "ChunkServer.RPCPushDataAndForward", gfs.PushDataAndForwardArg{data, l.Secondaries}, &d)
	if err != nil {
		return err
	}

	wcargs := gfs.WriteChunkArg{handle, offset, d.DataID, l.Secondaries}
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

	var d gfs.PushDataAndForwardReply
	err = util.Call(l.Primary, "ChunkServer.RPCPushDataAndForward", gfs.PushDataAndForwardArg{data, l.Secondaries}, &d)
	if err != nil {
		return
	}

	var a gfs.AppendChunkReply
	acargs := gfs.AppendChunkArg{handle, d.DataID, l.Secondaries}
	err = util.Call(l.Primary, "ChunkServer.RPCAppendChunk", acargs, &a)
	offset = a.Offset
	return
}

func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {

}
