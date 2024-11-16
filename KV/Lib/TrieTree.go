package Lib

type TrieNode[T any] struct {
	child    map[string]*TrieNode[T]
	Value    T
	HasValue bool
}

func NewTrieNode[T any]() *TrieNode[T] {
	return &TrieNode[T]{
		child: make(map[string]*TrieNode[T]),
	}
}

func (n *TrieNode[T]) Add(key []string, value T) {
	if n == nil {
		panic("impossible")
	}
	if len(key) == 0 {
		n.Value = value
		n.HasValue = true
		return
	}
	child, ok := n.child[key[0]]
	if !ok {
		child = NewTrieNode[T]()
		n.child[key[0]] = child
	}
	child.Add(key[1:], value)
}

func (n *TrieNode[T]) Match(key []string) (T, bool) {
	var t T
	if n == nil {
		return t, false
	}
	if len(key) == 0 {
		return n.Value, n.HasValue
	}
	child, ok := n.child[key[0]]
	if !ok {
		return t, false
	}
	return child.Match(key[1:])
}
