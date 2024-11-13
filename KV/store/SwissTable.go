package store

import (
	csmap "github.com/mhmtszr/concurrent-swiss-map"
)

type SwissTableStore struct {
	*csmap.CsMap[string, string]
}

func NewSwissTableStore() *SwissTableStore {
	m := csmap.Create[string, string](
		csmap.WithShardCount[string, string](32),
		// csmap.WithCustomHasher[string, string](func(key string) uint64 {
		// 	hash := fnv.New64a()
		// 	hash.Write([]byte(key))
		// 	return hash.Sum64()
		// }),
		csmap.WithSize[string, string](1000),
	)
	return &SwissTableStore{m}
}

func (st *SwissTableStore) Get(key string) (string, error) {
	v, ok := st.CsMap.Load(key)
	if !ok {
		return "", ErrKeyNotFound
	}
	return v, nil
}

func (st *SwissTableStore) Set(key, value string) error {
	st.CsMap.Store(key, value)
	return nil
}

func (st *SwissTableStore) Del(key string) error {
	st.CsMap.Delete(key)
	return nil
}

func (st *SwissTableStore) SerializeAll() ([]byte, error) {
	return st.CsMap.MarshalJSON()
}

func (st *SwissTableStore) Load(data []byte) error {
	return st.CsMap.UnmarshalJSON(data)
}
