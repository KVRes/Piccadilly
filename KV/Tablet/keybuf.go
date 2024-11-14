package Tablet

type KeyBuf struct {
	keys []string
	w    chan struct {
		idx int
		key string
	}
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

func (kb *KeyBuf) Write(idx int, val string) {
	kb.w <- struct {
		idx int
		key string
	}{idx, val}
}

func newKeyBuf(size int) *KeyBuf {
	kb := KeyBuf{}
	kb.keys = make([]string, size)
	go func() {
		for {
			w := <-kb.w
			kb.keys[w.idx] = w.key
		}
	}()
	return &kb
}
