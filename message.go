package main

import (
	"net"
	"encoding/binary"
	"time"
)

func MakeLockRequest(ID string, key uint64) []byte {
	arr := make([]byte, 17)
	arr[0] = MLockReq
	binary.PutUvarint(arr[1:], key)
	binary.PutUvarint(arr[9:], time.Now().UnixNano())
	return arr
}

func MakeUpdateRequest(ID string, key uint64, val string) []byte {
	arr := make([]byte, 17 + len(val))
	arr[0] = MUpdateVal
	binary.PutUvarint(arr[1:], key)
	binary.PutUvarint(arr[9:], uint64(len(val)))
	copy(arr[17:], []byte(val))
	return arr
}
