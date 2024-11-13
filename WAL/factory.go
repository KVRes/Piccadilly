package WAL

import "errors"

type Type string

const (
	JsonWAL Type = "json"
	FakeWAL Type = "fake"
)

func NewWAL(walType Type, path string) (Provider, error) {
	switch walType {
	case JsonWAL:
		return NewJsonWALProvider(path)
	case FakeWAL:
		return NewFakeWALProvider(), nil
	}
	return nil, errors.New("unknown wal type")
}
