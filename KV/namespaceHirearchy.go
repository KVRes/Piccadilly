package KV

import (
	"errors"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/WAL"
	"github.com/KVRes/Piccadilly/store"
	"github.com/KVRes/Piccadilly/utils"
	"os"
	"sync"
	"time"
)

type PNode struct {
	Bkt      *Tablet.Bucket
	LoadTime int64
	Started  bool
}

type Namespace map[string]*PNode

type Database struct {
	nsLck     sync.RWMutex
	Namespace Namespace
	basePath  string
}

func NewDatabase(basePath string) *Database {
	return &Database{
		Namespace: make(Namespace),
		basePath:  basePath,
	}
}

type ConcurrentModel int

const (
	Linear ConcurrentModel = iota
	Buffer
)

func (d *Database) Connect(path string, c ConnectStrategy, concu ConcurrentModel) (*PNode, error) {
	path = pathToNamespace(path)
	pnode := d.NsGet(path)
	var err error
	if pnode == nil || pnode.LoadTime == 0 {
		pnode, err = d.loadNamespace(path, c)
		if err != nil {
			return nil, err
		}
	}
	if pnode.Started {
		return pnode, nil
	}

	d.nsLck.Lock()
	defer d.nsLck.Unlock()
	if pnode.Started {
		return pnode, nil
	}
	wKeySet := 100
	if concu == Linear {
		wKeySet = 1
	}
	err = pnode.Bkt.StartService(Tablet.BucketConfig{
		WALPath:       d.WALPath(path),
		PersistPath:   d.PersistPath(path),
		FlushInterval: 5 * time.Second,
		WBuffer:       100,
		WKeySet:       wKeySet,
		NoFlush:       false,
	})
	if err == nil {
		pnode.Started = true
	}
	return pnode, nil
}

type ConnectStrategy int

const (
	CreateIfNotExist ConnectStrategy = iota
	ErrorIfNotExist
)

func (d *Database) NsGet(path string) *PNode {
	d.nsLck.RLock()
	defer d.nsLck.RUnlock()
	return d._nsGet(path)
}

var ErrNotStarted = errors.New("not started")
var ErrNotLoaded = errors.New("not loaded")

func (d *Database) MustGetStartedPnode(path string) (*PNode, error) {
	pnode := d.NsGet(path)
	if pnode == nil || pnode.LoadTime == 0 {
		return nil, ErrNotLoaded
	}
	if !pnode.Started {
		return nil, ErrNotStarted
	}
	return pnode, nil
}

func (d *Database) _nsGet(path string) *PNode {
	return d.Namespace[pathToNamespace(path)]
}

func (d *Database) loadNamespace(path string, c ConnectStrategy) (*PNode, error) {
	d.nsLck.Lock()
	defer d.nsLck.Unlock()
	pnode := d._nsGet(path)
	if pnode == nil {
		pnode = &PNode{}
		d.Namespace[pathToNamespace(path)] = pnode
	}
	if pnode.LoadTime != 0 {
		return pnode, nil
	}
	if !utils.IsExist(d.NamespacePath(path)) {
		if c != CreateIfNotExist {
			return nil, os.ErrNotExist
		}
		os.MkdirAll(d.NamespacePath(path), 0755)
	}

	wal, err := WAL.NewJsonWALProvider(d.WALPath(path))
	if err != nil {
		return pnode, err
	}

	pnode.Bkt = Tablet.NewBucket(store.NewSwissTableStore(), wal)
	pnode.LoadTime = time.Now().Unix()
	return pnode, nil
}
