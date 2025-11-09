package storage

import "encoding/binary"

func logKey(index uint64) []byte {
	key := make([]byte, len(keyLogPrefix)+8)
	copy(key, keyLogPrefix)
	binary.BigEndian.PutUint64(key[len(keyLogPrefix):], index)
	return key
}

func uint64ToBytes(val uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return buf
}

func bytesToUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
