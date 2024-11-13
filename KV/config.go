package KV

import (
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/KV/store"
	"time"
)

type DatabaseTemplate struct {
	KeySize       int
	WBuffer       int
	FlushInterval time.Duration
	NoFlush       bool
	WALType       WAL.Type
	StoreType     store.Type
}

func DefaultDatabaseTemplate() DatabaseTemplate {
	return DatabaseTemplate{
		KeySize:       5,
		WBuffer:       128,
		FlushInterval: 1 * time.Minute,
		NoFlush:       false,
		WALType:       WAL.JsonWAL,
		StoreType:     store.SwissTable,
	}
}
