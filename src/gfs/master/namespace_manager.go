package master

import (
	"fmt"
	//"path"
	"strings"
	"sync"

	"gfs"
	log "github.com/Sirupsen/logrus"
)

type namespaceManager struct {
	root     *nsTree
	serialCt int
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

type serialTreeNode struct {
	IsDir    bool
	Children map[string]int
	Chunks   int64
}

// tree2array transforms the namespace tree into an array for serialization
func (nm *namespaceManager) tree2array(array *[]serialTreeNode, node *nsTree) int {
	n := serialTreeNode{IsDir: node.isDir, Chunks: node.chunks}
	if node.isDir {
		n.Children = make(map[string]int)
		for k, v := range node.children {
			n.Children[k] = nm.tree2array(array, v)
		}
	}

	*array = append(*array, n)
	ret := nm.serialCt
	nm.serialCt++
	return ret
}

// Serializa the metadata for storing to disk
func (nm *namespaceManager) Serialize() []serialTreeNode {
	nm.root.RLock()
	defer nm.root.RUnlock()

	nm.serialCt = 0
	var ret []serialTreeNode
	nm.tree2array(&ret, nm.root)
	return ret
}

// array2tree transforms the an serialized array to namespace tree
func (nm *namespaceManager) array2tree(array []serialTreeNode, id int) *nsTree {
	n := &nsTree{
		isDir:  array[id].IsDir,
		chunks: array[id].Chunks,
	}

	if array[id].IsDir {
		n.children = make(map[string]*nsTree)
		for k, v := range array[id].Children {
			n.children[k] = nm.array2tree(array, v)
		}
	}

	return n
}

// Deserializa the metadata from disk
func (nm *namespaceManager) Deserialize(array []serialTreeNode) error {
	nm.root.Lock()
	defer nm.root.Unlock()
	nm.root = nm.array2tree(array, len(array)-1)
	return nil
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
func (nm *namespaceManager) lockParents(p gfs.Path, goDown bool) ([]string, *nsTree, error) {
	ps := strings.Split(string(p), "/")[1:]
	cwd := nm.root
	//log.Info("ps ", ps, " len: ", len(ps))
	if len(ps) > 0 {
		cwd.RLock()
		//log.Info("lock root")
		for i, name := range ps[:len(ps)] {
			// TODO : check path name
			c, ok := cwd.children[name]
			if !ok {
				return ps, cwd, fmt.Errorf("path %s not found", p)
			}
			if i == len(ps)-1 {
				if goDown { // go down deeper?
					cwd = c
				}
			} else {
				cwd = c
				//log.Info("lock ", name)
				cwd.RLock()
			}
		}
	}
	return ps, cwd, nil
}

// unlockParents remove read lock on all parents. If a parent does not exist,
// it just stops and returns. This is the inverse of lockParents.
func (nm *namespaceManager) unlockParents(ps []string) {
	cwd := nm.root
	if len(ps) > 0 {
		cwd.RUnlock()
		for _, name := range ps[:len(ps)-1] {
			c, ok := cwd.children[name]
			if !ok {
				log.Fatal("error in unlock")
				return
			}
			cwd = c
			cwd.RUnlock()
		}
	}
}

// PartionLastName partions the last filename from p
// e.g. /foo/bar/haha.txt -> /foo/bar , haha.txt
func (nm *namespaceManager) PartionLastName(p gfs.Path) (gfs.Path, string) {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i], string(p[i+1:])
		}
	}
	return "", ""
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	var filename string
	p, filename = nm.PartionLastName(p)

	log.Info("create file ", p, "/", filename)

	ps, cwd, err := nm.lockParents(p, true)
	defer nm.unlockParents(ps)
	if err != nil {
		return err
	}

	cwd.Lock()
	defer cwd.Unlock()

	if _, ok := cwd.children[filename]; ok {
		return fmt.Errorf("path %s already exists", p)
	}
	cwd.children[filename] = new(nsTree)
	return nil
}

// Delete deletes an file on path p.
func (nm *namespaceManager) Delete(p gfs.Path) error {
	ps, cwd, err := nm.lockParents(p, false)
	defer nm.unlockParents(ps)
	if err != nil {
		return err
	}

	filename := ps[len(ps)-1]

	cwd.Lock()
	defer cwd.Unlock()

	// rename, laze delete
	node := cwd.children[filename]
	delete(cwd.children, filename)
	cwd.children[gfs.DeletedFilePrefix+filename] = node
	return nil
}

// Rename rename an file on path p.
func (nm *namespaceManager) Rename(source, target gfs.Path) error {
	log.Fatal("Unsupported Rename")
	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	var filename string
	p, filename = nm.PartionLastName(p)

	log.Info("mkdir ", p, "/", filename)

	ps, cwd, err := nm.lockParents(p, true)
	defer nm.unlockParents(ps)
	if err != nil {
		return err
	}

	cwd.Lock()
	defer cwd.Unlock()

	if _, ok := cwd.children[filename]; ok {
		return fmt.Errorf("path %s already exists", p)
	}
	cwd.children[filename] = &nsTree{isDir: true,
		children: make(map[string]*nsTree)}
	return nil
}

// List returns information of all files and directories inside p.
func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	log.Info("list ", p)

	var dir *nsTree
	if p == gfs.Path("/") {
		dir = nm.root
	} else {
		ps, cwd, err := nm.lockParents(p, true)
		defer nm.unlockParents(ps)
		if err != nil {
			return nil, err
		}
		dir = cwd
	}
	dir.RLock()
	defer dir.RUnlock()

	if !dir.isDir {
		return nil, fmt.Errorf("path %s is a file, not directory", p)
	}

	ls := make([]gfs.PathInfo, 0, len(dir.children))
	for name, v := range dir.children {
		ls = append(ls, gfs.PathInfo{
			Name:   name,
			IsDir:  v.isDir,
			Length: v.length,
			Chunks: v.chunks,
		})
	}
	return ls, nil
}
