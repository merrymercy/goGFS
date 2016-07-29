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
		return 0, gfs.Error{gfs.UnknownError, fmt.Sprintf("len(data) = %v > max append size %v", len(data), gfs.MaxAppendSize)}
	}

	var l gfs.GetPrimaryAndSecondariesReply
	err = util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{handle}, &l)
	if err != nil { return }

    log.Infof("Client : push data to primary %v", data[:2])
	var d gfs.PushDataAndForwardReply
	err = util.Call(l.Primary, "ChunkServer.RPCPushDataAndForward", gfs.PushDataAndForwardArg{handle, data, l.Secondaries}, &d)
	if err != nil { return }

    log.Infof("Client : send append request to primary. data : %v", d.DataID)
	var a gfs.AppendChunkReply
	acargs := gfs.AppendChunkArg{d.DataID, l.Secondaries}
	err = util.Call(l.Primary, "ChunkServer.RPCAppendChunk", acargs, &a)

    if a.ErrorCode == gfs.AppendExceedChunkSize {
        err = gfs.Error{a.ErrorCode, "append over chunks"}
    }
	offset = a.Offset
	return
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

    var f gfs.GetFileInfoReply
    err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{path}, &f)
    if err != nil { return }

    start := gfs.ChunkIndex(f.Chunks - 1)
    if (start < 0) { start = 0 }

    var chunkOffset gfs.Offset
    for {
        var h gfs.GetChunkHandleReply
        err = util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{path, start}, &h)
        if err != nil { return }

        chunkOffset, err = c.AppendChunk(h.Handle, data)
        if err == nil || err.(gfs.Error).Code != gfs.AppendExceedChunkSize {
            break
        }

        // retry in next chunk
        start++
        log.Warning("Try on next chunk ", start )
    }

    if err != nil { return }

    offset = gfs.Offset(start) * gfs.MaxChunkSize + chunkOffset
    return
}

func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
    begin := 0

    var f gfs.GetFileInfoReply
    err := util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{path}, &f)
    if err != nil { return err }

    if int64(offset / gfs.MaxChunkSize) > f.Chunks {
        return fmt.Errorf("offset exceed file size")
    }

    for {
        index := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
        chunkOffset := offset % gfs.MaxChunkSize

        var h gfs.GetChunkHandleReply
        err := util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{path, index}, &h)
        if err != nil { return err }

        var writeLen int
        if begin + gfs.MaxChunkSize > len(data) {
            writeLen = len(data) - begin
        } else {
            writeLen = gfs.MaxChunkSize
        }

        err = c.WriteChunk(h.Handle, chunkOffset, data[begin : begin + writeLen])

        offset += gfs.Offset(writeLen)
        begin  += writeLen

        if begin == len(data) { break }
    }

    return nil
}

func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {

    return 0, nil
}


    /*
    var f gfs.GetFileInfoReply

    err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{path}, &f)
    if err != nil { return }
    */
