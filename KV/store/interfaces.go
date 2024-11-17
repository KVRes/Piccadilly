package store

import (
	"errors"
	"github.com/KVRes/Piccadilly/types"
)

type Provider interface {
	Get(key string) (types.Value, error)
	Set(key string, value types.Value) error
	Del(key string) error
	SerializeAll() ([]byte, error)
	Load(data []byte) error
	Keys() ([]string, error)
}

var ErrKeyNotFound = errors.New("key not found")
