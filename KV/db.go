package KV

import (
	"errors"
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/types"
	"github.com/KVRes/Piccadilly/utils"
	"log"
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

	err = d.pullUpBkt(pnode, path, concu)
	return pnode, path, err
}

func (d *Database) pullUpBkt(pnode *PNode, path string, concu types.ConcurrentModel) error {
	d.NS.Lock()
	defer d.NS.Unlock()
	if pnode == nil {
		return nil
	}
	if pnode.Started {
		return nil
	}
	log.Printf("[Bkt %p] start service", pnode.Bkt)
	err := pnode.Bkt.StartService(Tablet.BucketConfig{
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
	return err
}

func (d *Database) MustGetStartedPNode(path string) (*PNode, error) {
	path = pathToNamespace(path)
	pnode := d.NS.Get(path)
	var err error
	if pnode == nil || pnode.LoadTime == 0 {
		pnode, err = d.loadNamespace(path, types.ErrorIfNotExist)
		if err != nil {
			return nil, err
		}
	}
	if !pnode.Started {
		err = d.pullUpBkt(pnode, path, types.NoLinear)
		if err != nil {
			return nil, err
		}
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
		log.Printf("[Master] created path: %s", path)
		_ = os.MkdirAll(d.namespacePath(path), 0755)
	}

	wal, err := WAL.NewWAL(d.Template.WALType, d.walPath(path))
	if err != nil {
		return pnode, err
	}

	str, err := Store.NewStore(d.Template.StoreType)
	if err != nil {
		return pnode, err
	}
	log.Printf("[Master] loaded bkt %p at pnode %s(%p)", pnode.Bkt, path, pnode)
	pnode.Bkt = Tablet.NewBucket(str, wal)
	pnode.LoadTime = time.Now().Unix()
	return pnode, nil
}
