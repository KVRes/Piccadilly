package engine

import (
	art "github.com/plar/go-adaptive-radix-tree"
)

type ART struct {
	tree art.Tree
}

func ARTTree() *ART {
	return &ART{tree: art.New()}
}
