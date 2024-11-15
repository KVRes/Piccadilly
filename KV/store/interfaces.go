package store

import "errors"

type Provider interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Del(key string) error
	SerializeAll() ([]byte, error)
	Load(data []byte) error
	Keys() ([]string, error)
}

var ErrKeyNotFound = errors.New("key not found")
