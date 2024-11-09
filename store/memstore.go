package store

import (
	"encoding/json"
	"sync"
)

type MemStore struct {
	l sync.RWMutex
	m map[string]string
}

func NewMemStore() *MemStore {
	return &MemStore{m: make(map[string]string)}
}

func (m *MemStore) Set(key, value string) error {
	m.l.Lock()
	defer m.l.Unlock()
	m.m[key] = value
	return nil
}

func (m *MemStore) Get(key string) (string, error) {
	m.l.RLock()
	defer m.l.RUnlock()
	if value, ok := m.m[key]; ok {
		return value, nil
	}
	return "", nil
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

var _ Store = &MemStore{}
