package commitlog

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
)

type JsonFileProvider struct {
	l        sync.Mutex
	content  []Record
	filePath string
	file     *os.File
}

func NewJsonFileProvider(filePath string) (*JsonFileProvider, error) {
	obj := &JsonFileProvider{filePath: filePath}
	handle, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	obj.file = handle
	return obj, nil
}

func (j *JsonFileProvider) Append(record Record) (uint64, error) {
	j.l.Lock()
	defer j.l.Unlock()
	j.content = append(j.content, record)
	line, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}
	// new line
	line = append(line, '\n')
	_, err = j.file.Write(line)
	if err != nil {
		return 0, err
	}
	return uint64(len(j.content)), nil
}

func (j *JsonFileProvider) RecordsSinceLastChkptr() ([]Record, error) {
	checkpointPos, err := j.GetLastChkptr()
	if err != nil {
		return nil, err
	}
	return j.content[checkpointPos:], nil
}

func (j *JsonFileProvider) Truncate() error {
	j.l.Lock()
	defer j.l.Unlock()
	checkpointPos, err := j.GetLastChkptr()
	if err != nil {
		return err
	}
	j.content = j.content[checkpointPos:]
	return nil
}

func (j *JsonFileProvider) Load(data []byte) error {
	// text to json lines
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

func (j *JsonFileProvider) GetLastChkptr() (uint64, error) {
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

func (j *JsonFileProvider) Serialize() ([]byte, error) {
	lines := make([]string, 0, len(j.content))
	bs := &strings.Builder{}
	for _, record := range j.content {
		line, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(line))
	}
	bs.WriteString(strings.Join(lines, "\n"))
	return []byte(bs.String()), nil
}

var _ CommitLog = &JsonFileProvider{}
