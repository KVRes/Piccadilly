package KV

import "sync"

type Namespace struct {
	sync.RWMutex
	ns map[string]*PNode
}

func (ns *Namespace) Get(key string) *PNode {
	ns.RLock()
	defer ns.RUnlock()
	return ns.GetUnsafe(key)
}

func (ns *Namespace) GetE(key string) (*PNode, bool) {
	ns.RLock()
	defer ns.RUnlock()
	return ns.GetEUnsafe(key)
}

func (ns *Namespace) Set(key string, pnode *PNode) {
	ns.Lock()
	defer ns.Unlock()
	ns.SetUnsafe(key, pnode)
}

func (ns *Namespace) GetUnsafe(key string) *PNode {
	return ns.ns[key]
}

func (ns *Namespace) GetEUnsafe(key string) (*PNode, bool) {
	v, ok := ns.ns[key]
	return v, ok
}

func (ns *Namespace) SetUnsafe(key string, pnode *PNode) {
	ns.ns[key] = pnode
}
