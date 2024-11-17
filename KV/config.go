package KV

import (
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/types"
	"time"
)

type DatabaseTemplate struct {
	WBuffer       int
	FlushInterval time.Duration
	LongInterval  time.Duration
	NoFlush       bool
	WALType       WAL.Type
	StoreType     Store.Type
	WModel        types.ConcurrentModel
}

func DefaultDatabaseTemplate() DatabaseTemplate {
	return DatabaseTemplate{
		WBuffer:       128,
		FlushInterval: 1 * time.Minute,
		LongInterval:  5 * time.Minute,
		NoFlush:       false,
		WALType:       WAL.JsonWAL,
		StoreType:     Store.SwissTable,
		WModel:        types.NoLinear,
	}
}
