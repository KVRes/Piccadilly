package store

import "errors"

type Store interface {
	Get(key string) (string, error)
	Set(key, value string) error
	SerializeAll() ([]byte, error)
	Load(data []byte) error
}

var ErrKeyNotFound = errors.New("key not found")
