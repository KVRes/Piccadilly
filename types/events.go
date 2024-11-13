package types

type EventType int

const (
	EventAll EventType = iota
	EventSet
	EventDelete
)
