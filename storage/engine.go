package storage

type Engine interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error

	DeleteRange(start, end []byte) error

	GetTargetSizeKey(startKey []byte, endKey []byte, size uint64) (uint64, []byte, error)

	CreateSnapshot(path string, start, end []byte) error
	ApplySnapshot(path string) error

	Close() error
}
