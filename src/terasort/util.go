package terasort

import (
	"net/rpc"
	"os"
)

// Call is RPC call helper
func Call(srv, rpcname string, args, reply interface{}) error {
	c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err
}

const (
	DefaultBufferSize = 16 << 20
)

// buffered IO
type FileBuffer struct {
	file  *os.File
	buf   []byte
	align int
	size  int
	pos   int
}

func NewFileBuffer(filename string, align, size int) (*FileBuffer, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	size -= size % align
	buf := make([]byte, align*size)

	return &FileBuffer{file, buf, align, size, 0}, nil
}

func (fb *FileBuffer) Get() ([]byte, error) {
	n, err := fb.file.ReadAt(fb.buf, int64(fb.pos))
	fb.pos += n
	return fb.buf[:n], err
}

func (fb *FileBuffer) Destroy() {
	fb.file.Close()
}
