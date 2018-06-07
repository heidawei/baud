package badgerdb

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/dgraph-io/badger"
	"github.com/rubenv/gorocksdb"
)

var _ store.KVReader = &Reader{}

type Reader struct {
	tx   *gorocksdb.Snapshot
}

func NewReader(tx *gorocksdb.Snapshot) *Reader {
	return &Reader{tx: tx}
}

func (r *Reader)Get(key []byte) ([]byte, error) {
	gorocksdb.
	v, err := r.tx.(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return v.ValueCopy([]byte(nil))
}

// MultiGet retrieves multiple values in one call.
func (r *Reader)MultiGet(keys [][]byte) ([][]byte, error) {
	var vs [][]byte = make([][]byte, 0, len(keys))
	for _, key := range keys {
		v, err := r.Get(key)
		if err != nil {
			return nil, err
		}
		vs = append(vs, v)
	}
	return vs, nil
}

// PrefixIterator returns a KVIterator that will
// visit all K/V pairs with the provided prefix
func (r *Reader)PrefixIterator(prefix []byte) store.KVIterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := r.tx.NewIterator(opts)
	rv := &Iterator{
		tx:     r.tx,
		iter:   it,
		prefix: prefix,
	}

	rv.Seek(prefix)
	return rv
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r *Reader)RangeIterator(start, end []byte) store.KVIterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := r.tx.NewIterator(opts)
	rv := &Iterator{
		tx:    r.tx,
		iter:  it,
		start: start,
		end:   end,
	}

	rv.Seek(start)
	return rv
}

// Close closes the iterator
func (r *Reader)Close() error {
	r.tx.Discard()
	return nil
}