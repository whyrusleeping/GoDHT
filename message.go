package main

import (
	"encoding/binary"
)

const (
	MLockReq = byte(iota)
	MLockRespYes
	MLockRespNo
	MUpdateVal
	MLockRelease
	MRemindVal
	MRemindReq
)

func MakeLockRequest(ID []byte, key uint64, ts uint64) []byte {
	arr := make([]byte, 25 + len(ID))
	arr[0] = MLockReq
	binary.PutUvarint(arr[1:], key)
	binary.PutUvarint(arr[9:], ts)
	binary.PutUvarint(arr[17:], uint64(len(ID)))
	copy(arr[25:], ID)
	return arr
}

func MakeLockResponse(resp bool, ID []byte, key uint64, ts uint64) []byte {
	arr := make([]byte, 26 + len(ID))
	if resp {
		arr[0] = MLockRespYes
	} else {
		arr[0] = MLockRespNo
	}
	binary.PutUvarint(arr[1:], key)
	binary.PutUvarint(arr[9:], ts)
	binary.PutUvarint(arr[17:], uint64(len(ID)))
	copy(arr[25:], ID)

	return arr
}

func MakeUpdateRequest(ID []byte, key uint64, val string) []byte {
	arr := make([]byte, 25 + len(ID) + len(val))
	arr[0] = MUpdateVal
	binary.PutUvarint(arr[1:], key)
	binary.PutUvarint(arr[9:], uint64(len(ID)))
	copy(arr[17:], ID)
	binary.PutUvarint(arr[17 + len(ID):], uint64(len(val)))
	copy(arr[25 + len(ID):], []byte(val))
	return arr
}

func MakeLockReleaseRequest(ID []byte, key uint64, ts uint64) []byte {
	arr := make([]byte, 25 + len(ID))
	arr[0] = MLockReq
	binary.PutUvarint(arr[1:], key)
	binary.PutUvarint(arr[9:], ts)
	binary.PutUvarint(arr[17:], uint64(len(ID)))
	copy(arr[25:], ID)
	return arr
}

func MakeReminder(key uint64, val string) []byte {
	arr := make([]byte, 17 + len(val))
	arr[0] = MRemindVal
	binary.PutUvarint(arr[1:], key)
	binary.PutUvarint(arr[9:], uint64(len(val)))
	copy(arr[17:], []byte(val))
	return arr
}

func MakeReminderRequest(key uint64) []byte {
	arr := make([]byte, 9)
	arr[0] = MRemindReq
	binary.PutUvarint(arr[1:], key)
	return arr
}
