package main

import (
	"encoding/json"
	"fmt"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/serv"
	"github.com/KVRes/Piccadilly/types"
	"github.com/KevinZonda/GoX/pkg/iox"
	"github.com/KevinZonda/GoX/pkg/panicx"
	"time"
)

type Config struct {
	ListenAt string    `json:"listen_at"`
	DBPath   string    `json:"db_path"`
	Config   *DBConfig `json:"config"`
}

type DBConfig struct {
	WBuffer       *int                   `json:"w_buffer"`
	FlushInterval *int                   `json:"flush_interval"`
	LongInterval  *int                   `json:"long_interval"`
	NoFlush       *bool                  `json:"no_flush"`
	WALType       *WAL.Type              `json:"wal_type"`
	StoreType     *Store.Type            `json:"store_type"`
	WModel        *types.ConcurrentModel `json:"w_model"`
}

func (cfg *DBConfig) applyCfg(db *KV.Database) {
	if cfg == nil || db == nil {
		return
	}

	if cfg.WBuffer != nil {
		db.Template.WBuffer = *cfg.WBuffer
	}
	if cfg.FlushInterval != nil {
		db.Template.FlushInterval = time.Second * time.Duration(*cfg.FlushInterval)
	}
	if cfg.LongInterval != nil {
		db.Template.LongInterval = time.Second * time.Duration(*cfg.LongInterval)
	}
	if cfg.NoFlush != nil {
		db.Template.NoFlush = *cfg.NoFlush
	}
	if cfg.WALType != nil {
		db.Template.WALType = *cfg.WALType
	}
	if cfg.StoreType != nil {
		db.Template.StoreType = *cfg.StoreType
	}
	if cfg.WModel != nil {
		db.Template.WModel = *cfg.WModel
	}
}

func main() {
	fmt.Println("=============================================================")
	fmt.Println("    ____  _                     ___ ____         __ ___    __\n" +
		"   / __ \\(_)_____________ _____/ (_) / /_  __   / //_/ |  / /\n" +
		"  / /_/ / / ___/ ___/ __ `/ __  / / / / / / /  / ,<  | | / / \n" +
		" / ____/ / /__/ /__/ /_/ / /_/ / / / / /_/ /  / /| | | |/ /  \n" +
		"/_/   /_/\\___/\\___/\\__,_/\\__,_/_/_/_/\\__, /  /_/ |_| |___/   \n                                    /____/                   ")
	fmt.Println("=============================================================")
	fmt.Println("                  Piccadilly KV Server                       ")
	fmt.Println("                 by KevinZonda Research                      ")
	fmt.Println("             https://research.kevinzonda.com                 ")
	fmt.Println("=============================================================")

	bs, err := iox.ReadAllByte("config.json")
	panicx.NotNilErr(err)

	var cfg Config
	panicx.NotNilErr(json.Unmarshal(bs, &cfg))
	if cfg.ListenAt == "" {
		cfg.ListenAt = types.DEFAULT_ADDR
	}

	sv := serv.NewServer(cfg.DBPath)
	if cfg.Config != nil {
		cfg.Config.applyCfg(sv.Db)
	}

	fmt.Println("Starting listening at", cfg.ListenAt)

	panicx.NotNilErr(sv.ServeTCP(cfg.ListenAt))
}
