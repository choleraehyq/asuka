package dn

const (
	logKeyPrefix byte = 'l'
	dataKeyPrefix byte = 'd'
)

func encodeLogKey(key []byte) []byte {
	return append([]byte{logKeyPrefix}, key...)
}

func encodeDataKey(key []byte) []byte {
	return append([]byte{dataKeyPrefix}, key...)
}

func assert(cond bool) {
	if !cond {
		panic(cond)
	}
}