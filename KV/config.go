package KV

import (
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/KV/store"
	"github.com/KVRes/Piccadilly/types"
	"time"
)

type DatabaseTemplate struct {
	WBuffer       int
	FlushInterval time.Duration
	NoFlush       bool
	WALType       WAL.Type
	StoreType     store.Type
	WModel        types.ConcurrentModel
}

func DefaultDatabaseTemplate() DatabaseTemplate {
	return DatabaseTemplate{
		WBuffer:       128,
		FlushInterval: 1 * time.Minute,
		NoFlush:       false,
		WALType:       WAL.JsonWAL,
		StoreType:     store.SwissTable,
		WModel:        types.NoLinear,
	}
}
