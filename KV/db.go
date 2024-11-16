package KV

import (
	"errors"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/KV/store"
	"github.com/KVRes/Piccadilly/types"
	"github.com/KVRes/Piccadilly/utils"
	"os"
	"time"
)

type PNode struct {
	Bkt      *Tablet.Bucket
	LoadTime int64
	Started  bool
}

type Database struct {
	NS       *Namespace
	basePath string
	Template DatabaseTemplate
}

func NewDatabase(basePath string) *Database {
	return &Database{
		NS:       &Namespace{ns: make(map[string]*PNode)},
		basePath: basePath,
		Template: DefaultDatabaseTemplate(),
	}
}

func (d *Database) ListPNodes(path string) []string {
	path = d.NamespacePath(path)
	dirs, err := os.ReadDir(path)
	if err != nil {
		return nil
	}
	var dbs []string
	for _, dir := range dirs {
		if dir.IsDir() {
			dbs = append(dbs, dir.Name())
		}
	}
	return dbs
}

func (d *Database) CreatePNode(path string) error {
	path = d.NamespacePath(path)
	err := os.MkdirAll(path, 0755)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	return err
}

func (d *Database) Connect(path string, c types.ConnectStrategy, concu types.ConcurrentModel) (*PNode, string, error) {
	path = pathToNamespace(path)
	pnode := d.NS.Get(path)
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

	d.NS.Lock()
	defer d.NS.Unlock()
	if pnode.Started {
		return pnode, path, nil
	}
	err = pnode.Bkt.StartService(Tablet.BucketConfig{
		WALPath:       d.walPath(path),
		PersistPath:   d.persistPath(path),
		FlushInterval: d.Template.FlushInterval,
		LongInterval:  d.Template.LongInterval,
		WBuffer:       d.Template.WBuffer,
		NoFlush:       d.Template.NoFlush,
		WModel:        concu,
	})
	if err == nil {
		pnode.Started = true
	}
	return pnode, path, nil
}

func (d *Database) MustGetStartedPNode(path string) (*PNode, error) {
	path = pathToNamespace(path)
	pnode := d.NS.Get(path)
	if pnode == nil || pnode.LoadTime == 0 {
		return nil, ErrNotLoaded
	}
	if !pnode.Started {
		return nil, ErrNotStarted
	}
	return pnode, nil
}

func (d *Database) loadNamespace(path string, c types.ConnectStrategy) (*PNode, error) {
	d.NS.Lock()
	defer d.NS.Unlock()
	pnode := d.NS.GetUnsafe(path)
	if pnode == nil {
		pnode = &PNode{}
		d.NS.SetUnsafe(path, pnode)
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
