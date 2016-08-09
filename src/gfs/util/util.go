package util

import (
	"fmt"
	"math/rand"
	"net/rpc"

	"gfs"
)

// Call is RPC call helper
func Call(srv gfs.ServerAddress, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", string(srv))
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err
}

// CallAll applies the rpc call to all destinations.
func CallAll(dst []gfs.ServerAddress, rpcname string, args interface{}) error {
	ch := make(chan error)
	for _, d := range dst {
		go func(addr gfs.ServerAddress) {
			ch <- Call(addr, rpcname, args, nil)
		}(d)
	}
	errList := ""
	for _ = range dst {
		if err := <-ch; err != nil {
			errList += err.Error() + ";"
		}
	}

	if errList == "" {
		return nil
	} else {
		return fmt.Errorf(errList)
	}
}

// Sample randomly chooses k elements from {0, 1, ..., n-1}.
// n should not be less than k.
func Sample(n, k int) ([]int, error) {
	if n < k {
		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
	}
	return rand.Perm(n)[:k], nil
}
