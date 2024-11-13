package WAL

type FakeWAL struct {
}

func NewFakeWAL() *FakeWAL {
	return &FakeWAL{}
}

func (w *FakeWAL) Append(rec Record) (uint64, error) {
	return 0, nil
}

func (w *FakeWAL) RecordsSinceLastChkptr() ([]Record, error) {
	return nil, nil
}

func (w *FakeWAL) Truncate() error {
	return nil
}

func (w *FakeWAL) Load(data []byte) error {
	return nil
}

func (w *FakeWAL) Serialize() ([]byte, error) {
	return nil, nil
}

func (w *FakeWAL) Close() error {
	return nil
}

func (w *FakeWAL) GetLastChkptr() (int64, error) {
	return 0, nil
}

var _ Provider = &FakeWAL{}
