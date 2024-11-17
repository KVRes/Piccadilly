package WAL

type FakeWALProvider struct {
}

func NewFakeWALProvider() *FakeWALProvider {
	return &FakeWALProvider{}
}

func (w *FakeWALProvider) Append(rec Record) (uint64, error) {
	return 0, nil
}

func (w *FakeWALProvider) RecordsSinceLastChkptr() ([]Record, error) {
	return nil, nil
}

func (w *FakeWALProvider) Truncate() error {
	return nil
}

func (w *FakeWALProvider) Load(data []byte) error {
	return nil
}

func (w *FakeWALProvider) Serialize() ([]byte, error) {
	return nil, nil
}

func (w *FakeWALProvider) Close() error {
	return nil
}

func (w *FakeWALProvider) GetLastChkptr() (int64, error) {
	return 0, nil
}

var _ Provider = &FakeWALProvider{}
