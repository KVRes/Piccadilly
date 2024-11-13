package client

import (
	"github.com/KVRes/Piccadilly/types"
)

type ErrorableEvent struct {
	Event
	Err     error
	IsError bool
}

type Event struct {
	EventType types.EventType
	Key       string
}
