package WAL

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
)

type JsonWALProvider struct {
	l        sync.Mutex
	content  []Record
	filePath string
	file     *os.File
}

func NewJsonWALProvider(filePath string) (*JsonWALProvider, error) {
	obj := &JsonWALProvider{filePath: filePath}
	handle, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	obj.file = handle
	return obj, nil
}

func (j *JsonWALProvider) Append(record Record) (uint64, error) {
	j.l.Lock()
	defer j.l.Unlock()
	j.content = append(j.content, record)
	line, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}
	// new line
	ls := string(line) + "\n"
	_, err = j.file.WriteString(ls)
	if err != nil {
		return 0, err
	}
	return uint64(len(j.content)), nil
}

func (j *JsonWALProvider) RecordsSinceLastChkptr() ([]Record, error) {
	checkpointPos, err := j.GetLastChkptr()
	if err != nil {
		return nil, err
	}
	return j.content[checkpointPos:], nil
}

func (j *JsonWALProvider) Truncate() error {
	j.l.Lock()
	defer j.l.Unlock()
	checkpointPos, err := j.GetLastChkptr()
	if err != nil {
		return err
	}
	j.content = j.content[checkpointPos:]
	return nil
}

func (j *JsonWALProvider) Load(data []byte) error {
	j.l.Lock()
	defer j.l.Unlock()

	lines := strings.Split(string(data), "\n")
	records := make([]Record, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record Record
		err := json.Unmarshal([]byte(line), &record)
		if err != nil {
			// TODO: log error
			continue
		}
		records = append(records, record)
	}
	j.content = records
	return nil
}

func (j *JsonWALProvider) Close() error {
	return j.file.Close()
}

func (j *JsonWALProvider) GetLastChkptr() (uint64, error) {
	if len(j.content) == 0 {
		return 0, nil
	}
	okPos := -1
	for i := len(j.content) - 1; i >= 0; i-- {
		switch j.content[i].StateOper {
		case StateOperCheckpointOk:
			okPos = i
			continue
		case StateOperCheckpoint:
			if okPos != -1 {
				return uint64(i), nil
			}
		}
	}
	return 0, nil
}

func (j *JsonWALProvider) Serialize() ([]byte, error) {
	return json.Marshal(j.content)
}

var _ Provider = &JsonWALProvider{}
