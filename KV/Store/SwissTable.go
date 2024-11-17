package Store

import (
	"github.com/KVRes/Piccadilly/types"
	csmap "github.com/mhmtszr/concurrent-swiss-map"
)

type SwissTableStore struct {
	*csmap.CsMap[string, types.Value]
}

func NewSwissTableStore() *SwissTableStore {
	m := csmap.Create[string, types.Value](
		csmap.WithShardCount[string, types.Value](32),
		// csmap.WithCustomHasher[string, string](func(key string) uint64 {
		// 	hash := fnv.New64a()
		// 	hash.Write([]byte(key))
		// 	return hash.Sum64()
		// }),
		csmap.WithSize[string, types.Value](1000),
	)
	return &SwissTableStore{m}
}

func (st *SwissTableStore) Get(key string) (types.Value, error) {
	v, ok := st.CsMap.Load(key)
	if !ok {
		return types.Value{}, ErrKeyNotFound
	}
	return v, nil
}

func (st *SwissTableStore) Set(key string, value types.Value) error {
	st.CsMap.Store(key, value)
	return nil
}

func (st *SwissTableStore) Del(key string) error {
	st.CsMap.Delete(key)
	return nil
}

func (st *SwissTableStore) Keys() ([]string, error) {
	var keys []string
	st.CsMap.Range(func(key string, value types.Value) bool {
		keys = append(keys, key)
		return false
	})
	return keys, nil
}

func (st *SwissTableStore) SerializeAll() ([]byte, error) {
	return st.CsMap.MarshalJSON()
}

func (st *SwissTableStore) Load(data []byte) error {
	return st.CsMap.UnmarshalJSON(data)
}
