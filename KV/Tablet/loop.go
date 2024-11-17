package Tablet

import (
	"log"
	"time"
)

func (b *Bucket) loop(f func()) {
	for {
		select {
		case <-b.exit:
			return
		default:
			f()
		}
	}
}

func (b *Bucket) longDaemonThread() {
	if b.cfg.LongInterval <= 0 {
		return
	}

	b.loop(func() {
		time.Sleep(b.cfg.LongInterval)
		b.Watcher.GC()
		b.wal.Truncate()
	})
}

func (b *Bucket) flushThread() {
	if b.cfg.NoFlush || b.cfg.FlushInterval <= 0 {
		return
	}
	b.loop(func() {
		time.Sleep(b.cfg.FlushInterval)
		if !b.needFlush() {
			log.Printf("[Bkt %p] no need to flush", b)
			return
		}

		err := b.Flush()
		log.Printf("[Bkt %p] flushed, err: %v", b, err)
		if err != nil {
			log.Printf("[Bkt %p] flush failed: %v", b, err)
			return

		}
	})
}
