package types

type KVPair struct {
	Key   string
	Value string
}

type Value struct {
	Data string `json:"data"`
}

type KVPairV struct {
	Key   string `json:"k"`
	Value Value  `json:"v"`
}
