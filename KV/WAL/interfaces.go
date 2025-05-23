package WAL

import (
	"github.com/KVRes/Piccadilly/types"
)

type Provider interface {
	Append(record Record) (uint64, error)
	RecordsSinceLastChkptr() ([]Record, error)
	Truncate() error
	Load(data []byte) error
	Serialize() ([]byte, error)
	Close() error
}

type Record struct {
	StateOper StateOperType `json:"op"`
	Key       string        `json:"k"`
	Value     types.Value   `json:"v"`
}

type StateOperType string

const (
	StateOperSet          StateOperType = "set"
	StateOperDel          StateOperType = "del"
	StateOperClear        StateOperType = "clear"
	StateOperCheckpoint   StateOperType = "chk"   // now they data is persisting, the log can be truncated from here
	StateOperCheckpointOk StateOperType = "chkok" // only when an OK is received, the checkpoint is complete, and the log can be truncated
)

func NewStateOperRecord(oper StateOperType) Record {
	return Record{
		StateOper: oper,
	}
}

func NewStateOperFromEventTypes(event types.EventType) Record {
	switch event {
	case types.EventSet:
		return NewStateOperRecord(StateOperSet)
	case types.EventDelete:
		return NewStateOperRecord(StateOperDel)
	case types.EventClear:
		return NewStateOperRecord(StateOperClear)
	default:
		panic("unknown event type")
	}
}

func (r Record) WithKeyValue(key string, value types.Value) Record {
	r.Key = key
	r.Value = value
	return r
}
