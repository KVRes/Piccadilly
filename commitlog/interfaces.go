package commitlog

type CommitLog interface {
	Append(record Record) (uint64, error)
	RecordsSinceLastChkptr() ([]Record, error)
	Truncate() error
	Load(data []byte) error
}

type Record struct {
	StateOper StateOperType
	Key       string
	Value     string
}

type StateOperType string

const (
	StateOperSet          StateOperType = "set"
	StateOperDel          StateOperType = "del"
	StateOperCheckpoint   StateOperType = "chk"   // now they data is persisting, the log can be truncated from here
	StateOperCheckpointOk StateOperType = "chkok" // only when an OK is received, the checkpoint is complete, and the log can be truncated
)

func NewStateOperRecord(oper StateOperType) Record {
	return Record{
		StateOper: oper,
	}
}

func (r Record) WithKeyValue(key, value string) Record {
	r.Key = key
	r.Value = value
	return r
}
