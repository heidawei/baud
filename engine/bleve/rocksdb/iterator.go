package rocksdb

import (
	"bytes"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/rubenv/gorocksdb"
)

var _ store.KVIterator  = &Iterator{}

type Iterator struct {
	close  sync.Once
	iter   *gorocksdb.Iterator
	prefix []byte
	start  []byte
	end    []byte
	valid  bool
	key    []byte
	val    []byte
}

func (i *Iterator) updateValid() {
	i.valid = (i.key != nil)
	if i.valid {
		if i.prefix != nil {
			i.valid = bytes.HasPrefix(i.key, i.prefix)
		} else if i.end != nil {
			i.valid = bytes.Compare(i.key, i.end) < 0
		}
	}
}

func (i *Iterator) Seek(k []byte) {
	if i == nil {
		return
	}
	if i.start != nil && bytes.Compare(k, i.start) < 0 {
		k = i.start
	}
	if i.prefix != nil && !bytes.HasPrefix(k, i.prefix) {
		if bytes.Compare(k, i.prefix) < 0 {
			k = i.prefix
		} else {
			i.valid = false
			return
		}
	}
	i.iter.Seek(k)
	if i.iter.Valid() {
		i.key = i.iter.Key().Data()
		i.val = i.iter.Value().Data()
	} else {
		i.key = nil
		i.val = nil
	}
	i.updateValid()
}

func (i *Iterator) Next() {
	if i == nil {
		return
	}
	i.iter.Next()
	if i.iter.Valid() {
		i.key = i.iter.Key().Data()
		i.val = i.iter.Value().Data()
	} else {
		i.key = nil
		i.val = nil
	}
	i.updateValid()
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i == nil {
		return nil, nil, false
	}
	return i.key, i.val, i.valid
}

func (i *Iterator) Key() []byte {
	if i == nil {
		return nil
	}
	return i.key
}

func (i *Iterator) Value() []byte {
	if i == nil {
		return nil
	}
	return i.val
}

func (i *Iterator) Valid() bool {
	if i == nil {
		return false
	}
	return i.valid
}

func (i *Iterator) Close() error {
	if i == nil {
		return nil
	}
	i.close.Do(func() {
		if i.iter != nil {
			i.iter.Close()
		}
	})

	return nil
}
