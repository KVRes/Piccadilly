package KV

import (
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/KV/store"
	"github.com/KVRes/Piccadilly/types"
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
	Template  DatabaseTemplate
}

func NewDatabase(basePath string) *Database {
	return &Database{
		Namespace: make(Namespace),
		basePath:  basePath,
		Template:  DefaultDatabaseTemplate(),
	}
}

func (d *Database) Connect(path string, c types.ConnectStrategy, concu types.ConcurrentModel) (*PNode, string, error) {
	path = pathToNamespace(path)
	pnode := d.nsGet(path)
	var err error
	if pnode == nil || pnode.LoadTime == 0 {
		pnode, err = d.loadNamespace(path, c)
		if err != nil {
			return nil, "", err
		}
	}
	if pnode.Started {
		return pnode, path, nil
	}

	d.nsLck.Lock()
	defer d.nsLck.Unlock()
	if pnode.Started {
		return pnode, path, nil
	}
	err = pnode.Bkt.StartService(Tablet.BucketConfig{
		WALPath:       d.walPath(path),
		PersistPath:   d.persistPath(path),
		FlushInterval: d.Template.FlushInterval,
		WBuffer:       d.Template.WBuffer,
		NoFlush:       d.Template.NoFlush,
		WModel:        concu,
	})
	if err == nil {
		pnode.Started = true
	}
	return pnode, path, nil
}

func (d *Database) nsGet(path string) *PNode {
	d.nsLck.RLock()
	defer d.nsLck.RUnlock()
	return d.Namespace[path]
}

func (d *Database) MustGetStartedPnode(path string) (*PNode, error) {
	path = pathToNamespace(path)
	pnode := d.nsGet(path)
	if pnode == nil || pnode.LoadTime == 0 {
		return nil, ErrNotLoaded
	}
	if !pnode.Started {
		return nil, ErrNotStarted
	}
	return pnode, nil
}

func (d *Database) loadNamespace(path string, c types.ConnectStrategy) (*PNode, error) {
	d.nsLck.Lock()
	defer d.nsLck.Unlock()
	pnode := d.Namespace[path]
	if pnode == nil {
		pnode = &PNode{}
		d.Namespace[path] = pnode
	}
	if pnode.LoadTime != 0 {
		return pnode, nil
	}
	if !utils.IsExist(d.namespacePath(path)) {
		if c != types.CreateIfNotExist {
			return nil, os.ErrNotExist
		}
		_ = os.MkdirAll(d.namespacePath(path), 0755)
	}

	wal, err := WAL.NewWAL(d.Template.WALType, d.walPath(path))
	if err != nil {
		return pnode, err
	}

	str, err := store.NewStore(d.Template.StoreType)
	if err != nil {
		return pnode, err
	}
	pnode.Bkt = Tablet.NewBucket(str, wal)
	pnode.LoadTime = time.Now().Unix()
	return pnode, nil
}
