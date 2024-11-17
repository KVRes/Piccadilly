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

func (b *Bucket) loopWithInterval(f func(), interval time.Duration) {
	inv := time.After(interval)
	for {
		select {
		case <-b.exit:
			return
		case <-inv:
			f()
			inv = time.After(interval)
		}
	}
}

func (b *Bucket) longDaemonThread() {
	if b.cfg.LongInterval <= 0 {
		return
	}

	b.loopWithInterval(func() {
		b.Watcher.GC()
		b.wal.Truncate()
	}, b.cfg.LongInterval)
}

func (b *Bucket) flushThread() {
	if b.cfg.NoFlush || b.cfg.FlushInterval <= 0 {
		return
	}
	b.loopWithInterval(func() {
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
	}, b.cfg.FlushInterval)
}
