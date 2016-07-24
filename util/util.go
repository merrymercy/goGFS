package util

import (
	"fmt"
	"math/rand"
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

// CallAll applies the rpc call to all destinations.
func CallAll(dst []llgfs.ServerAddress, rpcname string, args interface{}) (err error) {
	ch := make(chan error)
	for _, d := range dst {
		go func(addr llgfs.ServerAddress) {
			ch <- Call(addr, rpcname, args, nil)
		}(d)
	}
	for _ = range dst {
		if e := <-ch; e != nil {
			err = e
		}
	}
	return
}

// Sample randomly chooses k elements from {0, 1, ..., n-1}.
// n should not be less than k.
func Sample(n, k int) ([]int, error) {
	if n < k {
		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
	}
	return rand.Perm(n)[:k], nil
}
