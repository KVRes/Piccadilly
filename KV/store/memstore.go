package store

import (
	"encoding/json"
	"github.com/KVRes/Piccadilly/types"
	"sync"
)

type MemStore struct {
	l sync.RWMutex
	m map[string]types.Value
}

func NewMemStore() *MemStore {
	return &MemStore{m: make(map[string]types.Value)}
}

func (m *MemStore) Set(key string, value types.Value) error {
	m.l.Lock()
	defer m.l.Unlock()
	m.m[key] = value
	return nil
}

func (m *MemStore) Get(key string) (types.Value, error) {
	m.l.RLock()
	defer m.l.RUnlock()
	if value, ok := m.m[key]; ok {
		return value, nil
	}
	return types.Value{}, nil
}

func (m *MemStore) Del(key string) error {
	m.l.Lock()
	defer m.l.Unlock()
	delete(m.m, key)
	return nil
}

func (m *MemStore) SerializeAll() ([]byte, error) {
	m.l.RLock()
	defer m.l.RUnlock()
	return json.Marshal(m.m)
}

func (m *MemStore) Load(data []byte) error {
	m.l.Lock()
	defer m.l.Unlock()
	return json.Unmarshal(data, &m.m)
}

func (m *MemStore) Keys() ([]string, error) {
	var keys []string
	m.l.RLock()
	defer m.l.RUnlock()
	for k := range m.m {
		keys = append(keys, k)
	}
	return keys, nil
}

var _ Provider = &MemStore{}
