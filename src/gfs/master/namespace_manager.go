package master

import (
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/abcdabcd987/llgfs/gfs"
)

type namespaceManager struct {
	root *nsTree
}

type nsTree struct {
	sync.RWMutex

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	length int64
	chunks int64
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{isDir: true},
	}
	return nm
}

// lockParents place read lock on all parents of p. It returns the list of
// parents' name, the direct parent nsTree. If a parent does not exist,
// an error is also returned.
func (nm *namespaceManager) lockParents(p string) ([]string, *nsTree, error) {
	ps := strings.Split(path.Clean(p), "/")
	cwd := nm.root
	cwd.RLock()
	for _, name := range ps[:len(ps)-1] {
		c, ok := cwd.children[name]
		if !ok {
			return ps, cwd, fmt.Errorf("path %s not found", p)
		}
		cwd = c
		cwd.RLock()
	}
	return ps, cwd, nil
}

// unlockParents remove read lock on all parents. If a parent does not exist,
// it just stops and returns. This is the inverse of lockParents.
func (nm *namespaceManager) unlockParents(ps []string) {
	cwd := nm.root
	cwd.RUnlock()
	for _, name := range ps[:len(ps)-1] {
		c, ok := cwd.children[name]
		if !ok {
			return
		}
		cwd = c
		cwd.RUnlock()
	}
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p string) error {
	ps, cwd, err := nm.lockParents(p)
	defer nm.unlockParents(ps)
	if err != nil {
		return err
	}
	if _, ok := cwd.children[p]; ok {
		return fmt.Errorf("path %s already exists", p)
	}
	cwd.children[p] = new(nsTree)
	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p string) error {
	ps, cwd, err := nm.lockParents(p)
	defer nm.unlockParents(ps)
	if err != nil {
		return err
	}
	if _, ok := cwd.children[p]; ok {
		return fmt.Errorf("path %s already exists", p)
	}
	cwd.children[p] = &nsTree{isDir: true}
	return nil
}

// List returns information of all files and directories inside p.
func (nm *namespaceManager) List(p string) ([]gfs.PathInfo, error) {
	ps, cwd, err := nm.lockParents(p)
	defer nm.unlockParents(ps)
	if err != nil {
		return nil, err
	}
	cwd, ok := cwd.children[p]
	if !ok {
		return nil, fmt.Errorf("path %s does not exist", p)
	}
	if !cwd.isDir {
		return nil, fmt.Errorf("path %s is a file, not directory", p)
	}

	ls := make([]gfs.PathInfo, 0, len(cwd.children))
	for name, v := range cwd.children {
		ls = append(ls, gfs.PathInfo{
			Name:   name,
			IsDir:  v.isDir,
			Length: v.length,
			Chunks: v.chunks,
		})
	}
	return ls, nil
}
