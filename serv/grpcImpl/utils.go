package grpcImpl

import (
	"errors"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/Watcher"
)

var ErrNilBucket = errors.New("nil bucket")

type INamespaceGetter interface {
	GetNamespace() string
}

func getBkt(db *KV.Database, namespace INamespaceGetter) (bkt *Tablet.Bucket, err error) {
	path := namespace.GetNamespace()
	pnode, err := db.MustGetStartedPNode(path)
	if err != nil {
		return nil, err
	}
	bkt = pnode.Bkt
	return bkt, nil
}

func notNilBkt(bkt *Tablet.Bucket) (*Tablet.Bucket, error) {
	if bkt == nil {
		return nil, ErrNilBucket
	}
	return bkt, nil
}

func notNilWatcher(w *Watcher.KeyWatcher) (*Watcher.KeyWatcher, error) {
	if w == nil {
		return nil, ErrNilBucket
	}
	return w, nil
}
