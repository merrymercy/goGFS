package master

import (
	"fmt"
	//"path"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"gfs"
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
        root: &nsTree{isDir: true,
                      children: make(map[string]*nsTree)},
	}
    log.Info("-----------new namespace manager")
	return nm
}

// lockParents place read lock on all parents of p. It returns the list of
// parents' name, the direct parent nsTree. If a parent does not exist,
// an error is also returned.
func (nm *namespaceManager) lockParents(p gfs.Path) ([]string, *nsTree, error) {
    ps := strings.Split(string(p), "/")[1:]
	cwd := nm.root
    if len(ps) > 1 {
        cwd.RLock()
        for _, name := range ps[:len(ps)-1] {
            c, ok := cwd.children[name]
            if !ok { return ps, cwd, fmt.Errorf("path %s not found", p) }
            cwd = c
            cwd.RLock()
        }
    }
	return ps, cwd, nil
}

// unlockParents remove read lock on all parents. If a parent does not exist,
// it just stops and returns. This is the inverse of lockParents.
func (nm *namespaceManager) unlockParents(ps []string) {
	cwd := nm.root
    if len(ps) > 1 {
        cwd.RUnlock()
        for _, name := range ps[:len(ps)-1] {
            c, ok := cwd.children[name]
            if !ok { return }
            cwd = c
            cwd.RUnlock()
        }
    }
}

func (nm *namespaceManager) PartionLastName(p gfs.Path) (gfs.Path, string) {
    for i := len(p) - 1; i >= 0; i-- {
        if p[i] == '/' {
            return p[:i], string(p[i + 1:])
        }
    }
    return "", ""
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
    var filename string
    p, filename = nm.PartionLastName(p)

	ps, cwd, err := nm.lockParents(p)
	defer nm.unlockParents(ps)
	if err != nil { return err }

    if (len(ps) > 0) {
        var ok bool
        cwd, ok = cwd.children[ps[len(ps)-1]]
        if !ok {
            return fmt.Errorf("path %s does not exist", ps[len(ps)-1])
        }
    }
    cwd.Lock()
    defer cwd.Unlock()

	if _, ok := cwd.children[filename]; ok {
		return fmt.Errorf("path %s already exists", p)
	}
	cwd.children[filename] = new(nsTree)
	return nil
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Delete(p gfs.Path) error {
    return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
    var filename string
    p, filename = nm.PartionLastName(p)

	ps, cwd, err := nm.lockParents(p)
	defer nm.unlockParents(ps)
	if err != nil { return err }

    if (len(ps) > 0) {
        var ok bool
        cwd, ok = cwd.children[ps[len(ps)-1]]
        if !ok {
            return fmt.Errorf("path %s does not exist", ps[len(ps)-1])
        }
    }

    cwd.Lock()
    defer cwd.Unlock()

	if _, ok := cwd.children[filename]; ok {
		return fmt.Errorf("path %s already exists", p)
	}
    cwd.children[filename] = &nsTree{isDir:    true,
                                     children: make(map[string]*nsTree)}
	return nil
}

// List returns information of all files and directories inside p.
func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	ps, cwd, err := nm.lockParents(p)
	defer nm.unlockParents(ps)
	if err != nil { return nil, err }

    name := ps[len(ps)-1]
	cwd, ok := cwd.children[name]
	if !ok { return nil, fmt.Errorf("path %s does not exist", p) }

    cwd.RLock()
    defer cwd.RUnlock()

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
