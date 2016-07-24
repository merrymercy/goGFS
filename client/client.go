package client

import (
	"fmt"
	"github.com/abcdabcd987/llgfs"
	"github.com/abcdabcd987/llgfs/util"
)

// Client struct is the GFS client-side driver
type Client struct {
	master llgfs.ServerAddress
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path llgfs.Path, index llgfs.ChunkIndex) (llgfs.ChunkHandle, error) {
	var reply llgfs.GetChunkHandleReply
	err := util.Call(c.master, "Master.RPCGetChunkHandle", llgfs.GetChunkHandleArg{path, index}, &reply)
	if err != nil {
		return 0, err
	}
	return reply.Handle, nil
}

// WriteChunk writes data to the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) WriteChunk(handle llgfs.ChunkHandle, offset llgfs.Offset, data []byte) error {
	if len(data)+int(offset) > llgfs.MaxChunkSize {
		return fmt.Errorf("len(data)+offset = %v > max chunk size %v", len(data)+int(offset), llgfs.MaxChunkSize)
	}
	var l llgfs.GetPrimaryAndSecondariesReply
	err := util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", llgfs.GetPrimaryAndSecondariesArg{handle}, &l)
	if err != nil {
		return err
	}

	var d llgfs.PushDataAndForwardReply
	err = util.Call(l.Primary, "ChunkServer.RPCPushDataAndForward", llgfs.PushDataAndForwardArg{data, l.Secondaries}, &d)
	if err != nil {
		return err
	}

	wcargs := llgfs.WriteChunkArg{handle, offset, d.DataID, l.Secondaries}
	err = util.Call(l.Primary, "ChunkServer.RPCWriteChunk", wcargs, &llgfs.WriteChunkReply{})
	return err
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// <code>len(data)</code> should be within 1/4 chunk size.
func (c *Client) AppendChunk(handle llgfs.ChunkHandle, data []byte) (offset llgfs.Offset, err error) {
	if len(data) > llgfs.MaxAppendSize {
		return 0, fmt.Errorf("len(data) = %v > max append size %v", len(data), llgfs.MaxAppendSize)
	}
	var l llgfs.GetPrimaryAndSecondariesReply
	err = util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", llgfs.GetPrimaryAndSecondariesArg{handle}, &l)
	if err != nil {
		return
	}

	var d llgfs.PushDataAndForwardReply
	err = util.Call(l.Primary, "ChunkServer.RPCPushDataAndForward", llgfs.PushDataAndForwardArg{data, l.Secondaries}, &d)
	if err != nil {
		return
	}

	var a llgfs.AppendChunkReply
	acargs := llgfs.AppendChunkArg{handle, d.DataID, l.Secondaries}
	err = util.Call(l.Primary, "ChunkServer.RPCAppendChunk", acargs, &a)
	offset = a.Offset
	return
}
