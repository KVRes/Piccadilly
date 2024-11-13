package KV

import "errors"

type ConnectStrategy int

const (
	CreateIfNotExist ConnectStrategy = iota
	ErrorIfNotExist
)

var ErrNotStarted = errors.New("not started")
var ErrNotLoaded = errors.New("not loaded")

type ConcurrentModel int

const (
	Linear ConcurrentModel = iota
	Buffer
)
