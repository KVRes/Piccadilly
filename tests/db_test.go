package tests

import (
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/types"
	"os"
	"testing"
)

func TestDB(t *testing.T) {
	db := initDB()

	m := dataset()
	for k, v := range m {
		err := db.Set(k, types.Value{Data: v})
		if err != nil {
			t.Fatal(err)
		}
	}

	inDb(t, db, m)

	_ = db.Flush()
}

func TestDBPersist(t *testing.T) {
	db := initDB()

	m := dataset()
	inDb(t, db, m)
}

const WAL_NEED_REC = `
{"StateOper":"set","Key":"SPECTIAL","Value":"VALUE"}
{"StateOper":"set","Key":"SPEC","Value":"VAL"}
{"StateOper":"chk","Key":"","Value":""}
{"StateOper":"set","Key":"MIDDLE","Value":"NEED!"}
{"StateOper":"chkok","Key":"","Value":""}
{"StateOper":"set","Key":"key211","Value":"value212"}
{"StateOper":"set","Key":"key231","Value":"value232"}
{"StateOper":"set","Key":"key634","Value":"value635"}
{"StateOper":"set","Key":"key657","Value":"value658"}`

func TestRecoverFromLog(t *testing.T) {
	const WAL_F = "WAL_REC.json"
	const PERSIST_F = "persist_REC.json"
	// write to log
	os.WriteFile(WAL_F, []byte(WAL_NEED_REC), 0644)
	wal, err := WAL.NewJsonWALProvider(WAL_F)
	if err != nil {
		panic(err)
	}

	db := Tablet.NewBucket(Store.NewSwissTableStore(), wal)
	err = db.StartService(Tablet.BucketConfig{
		WALPath:     WAL_F,
		PersistPath: PERSIST_F,
	})

	if err != nil {
		panic(err)
	}

	m := map[string]string{
		"key211": "value212",
		"key231": "value232",
		"key634": "value635",
		"key657": "value658",
	}

	inDb(t, db, m)

	spc := map[string]string{
		"SPECTIAL": "VALUE",
		"SPEC":     "VAL",
	}
	notInDb(t, db, spc)

	mid := map[string]string{
		"MIDDLE": "NEED!",
	}
	inDb(t, db, mid)

}
