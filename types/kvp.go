package types

import "time"

type KVPair struct {
	Key   string
	Value string
}

type Value struct {
	Data   string `json:"d"`
	Expire *int64 `json:"exp,omitempty"`
}

func (v Value) Equals(o Value) bool {
	if v.Data != o.Data {
		return false
	}
	// nil, ?
	if v.Expire == nil {
		return o.Expire == nil
	}
	// v, nil
	if o.Expire == nil {
		return false
	}
	// vv
	return *v.Expire == *o.Expire
}

func (v Value) IsExpired() bool {
	if v.Expire == nil {
		return false
	}
	return *v.Expire < time.Now().Unix()
}

type KVPairV struct {
	Key   string `json:"k"`
	Value Value  `json:"v"`
}
