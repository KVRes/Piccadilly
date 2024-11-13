package store

import "errors"

type Type string

const (
	MemTable   Type = "memTable"
	SwissTable Type = "swissTable"
)

func NewStore(storeType Type) (Provider, error) {
	switch storeType {
	case MemTable:
		return NewMemStore(), nil
	case SwissTable:
		return NewSwissTableStore(), nil
	}
	return nil, errors.New("unknown store type")
}
