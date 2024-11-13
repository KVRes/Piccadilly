package utils

import "strings"

func TrimBytes(s []byte) []byte {
	str := string(s)
	return []byte(strings.TrimSpace(str))
}

func IsEmptyBytes(s []byte) bool {
	str := string(s)
	return str == ""
}
