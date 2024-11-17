package Tablet

import "github.com/KVRes/Piccadilly/types"

func toReq(kvp types.KVPairV, t types.EventType) internalReq {
	return internalReq{
		KVPairV: kvp,
		done:    make(chan error),
		t:       t,
	}
}

type internalReq struct {
	t types.EventType
	types.KVPairV
	done chan error
}

func (wr *internalReq) Close() {
	close(wr.done)
}
