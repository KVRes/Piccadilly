package types

type ConnectStrategy int

const (
	CreateIfNotExist ConnectStrategy = iota
	ErrorIfNotExist
)

type ConcurrentModel string

const (
	Linear   ConcurrentModel = "linear"
	NoLinear ConcurrentModel = "nolinear"
)

const (
	LinearGRPC int32 = iota
	NoLinearGRPC
)

func ConcurrentModelI32Cov(m int32) ConcurrentModel {
	switch m {
	case LinearGRPC:
		return Linear
	case NoLinearGRPC:
		return NoLinear
	default:
		return Linear
	}
}

func ConcurrentModelToI32(m ConcurrentModel) int32 {
	switch m {
	case Linear:
		return LinearGRPC
	case NoLinear:
		return NoLinearGRPC
	default:
		return LinearGRPC
	}
}
