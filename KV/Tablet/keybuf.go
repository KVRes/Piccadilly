package Tablet

type KeyBuf struct {
	keys []string
}

func (kb *KeyBuf) findEmpty(key string) int {
	empty := -1
	for i, k := range kb.keys {
		if k == key {
			return -1
		}
		if k == "" {
			empty = i
		}
	}

	return empty
}

func newKeyBuf(size int) *KeyBuf {
	kb := KeyBuf{}
	kb.keys = make([]string, size)
	return &kb
}
