package badger

import (
	"bytes"
	"io/ioutil"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"github.com/choleraehyq/asuka/pb/storagepb"
	"github.com/choleraehyq/asuka/storage"
)

var _ storage.Engine = (*badgerEngine)(nil)

type badgerEngine struct {
	db *badger.DB
}

func New(path string) (storage.Engine, error) {
	opts := badger.DefaultOptions
	opts.Dir, opts.ValueDir = path, path
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "New badgerEngine error")
	}
	return &badgerEngine{
		db: db,
	}, nil
}

func (e *badgerEngine) Set(key []byte, value []byte) error {
	return e.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (e *badgerEngine) Get(key []byte) ([]byte, error) {
	var v []byte
	return v, e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		v = make([]byte, len(val))
		copy(v, val)
		return nil
	})
}

func (e *badgerEngine) Delete(key []byte) error {
	return e.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (e *badgerEngine) DeleteRange(start, end []byte) error {
	return e.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		it.Seek(start)
		var items []*badger.Item
		for it.Valid() {
			if len(end) != 0 && bytes.Compare(it.Item().Key(), end) >= 0 {
				break
			}
			items = append(items, it.Item())
			it.Next()
		}
		for _, item := range items {
			if err := txn.Delete(item.Key()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (e *badgerEngine) GetTargetSizeKey(start []byte, end []byte, expectedSize uint64) (size uint64, key []byte, err error) {
	err = e.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		it.Seek(start)
		for it.Valid() {
			if len(end) != 0 && bytes.Compare(it.Item().Key(), end) >= 0 {
				break
			}
			v, err := it.Item().Value()
			if err != nil {
				return err
			}
			size += uint64(len(v))
			if size >= expectedSize {
				key = make([]byte, len(it.Item().Key()))
				copy(key, it.Item().Key())
				return nil
			}
			it.Next()
		}
		return nil
	})
	return
}

func (e *badgerEngine) CreateSnapshot(path string, start, end []byte) error {
	var snapshot storagepb.Snapshot
	err := e.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		it.Seek(start)
		for it.Valid() {
			if len(end) != 0 && bytes.Compare(it.Item().Key(), end) >= 0 {
				break
			}
			v, err := it.Item().Value()
			if err != nil {
				return err
			}
			value := make([]byte, len(v))
			copy(value, v)
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			pair := &storagepb.KVPair{
				Key:   key,
				Value: value,
			}
			snapshot.Data = append(snapshot.Data, pair)
			it.Next()
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "DB iteration in CreateSnapshot error")
	}
	b := make([]byte, snapshot.Size())
	_, err = snapshot.MarshalTo(b)
	if err != nil {
		return errors.Wrap(err, "Snapshot Marshal in CreateSnapshot error")
	}
	return ioutil.WriteFile(path, b, 0644)
}

func (e *badgerEngine) ApplySnapshot(path string) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrapf(err, "ReadFile %s in ApplySnapshot error", path)
	}
	var snapshot storagepb.Snapshot
	err = snapshot.Unmarshal(b)
	if err != nil {
		return errors.Wrap(err, "Unmarshal snapshot in ApplySnapshot error")
	}
	return e.db.Update(func(txn *badger.Txn) error {
		for _, pair := range snapshot.GetData() {
			if err := txn.Set(pair.GetKey(), pair.GetValue()); err != nil {
				return errors.Wrapf(err, "Set key %v value %v to database in ApplySnapshot error", pair.GetKey, pair.GetValue)
			}
		}
		return nil
	})
}

func (e *badgerEngine) Close() error {
	return e.db.Close()
}
