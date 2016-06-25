package util

import (
	"net/rpc"

	"github.com/abcdabcd987/llgfs"
)

// Call is RPC call helper
func Call(srv llgfs.ServerAddress, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", string(srv))
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}
