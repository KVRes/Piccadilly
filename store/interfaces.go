package store

type Store interface {
	Get(key string) (string, error)
	Set(key, value string) error
	SerializeAll() ([]byte, error)
	Load(data []byte) error
}
