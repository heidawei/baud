package rocksdb

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/rubenv/gorocksdb"
	"github.com/tiglabs/baudengine/util/bytes"
)

var _ store.KVReader = &Reader{}

type Reader struct {
	ops  *gorocksdb.ReadOptions
	snap *gorocksdb.Snapshot
	tx   *gorocksdb.DB
}

func NewReader(tx *gorocksdb.DB) *Reader {
	r := &Reader{tx: tx, ops: gorocksdb.NewDefaultReadOptions(), snap: tx.NewSnapshot()}
	r.ops.SetSnapshot(r.snap)
	return r
}

func (r *Reader)Get(key []byte) ([]byte, error) {
	v, err := r.tx.Get(r.ops, key)
	if err != nil {
		return nil, err
	}
	defer v.Free()
	if v.Size() == 0 {
		return nil, nil
	}
	return bytes.CloneBytes(v.Data()), nil
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
	it := r.tx.NewIterator(r.ops)
	rv := &Iterator{
		iter:   it,
		prefix: prefix,
	}

	rv.Seek(prefix)
	return rv
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r *Reader)RangeIterator(start, end []byte) store.KVIterator {
	it := r.tx.NewIterator(r.ops)
	rv := &Iterator{
		iter:  it,
		start: start,
		end:   end,
	}

	rv.Seek(start)
	return rv
}

// Close closes the iterator
func (r *Reader)Close() error {
	r.snap.Release()
	r.ops.Destroy()
	return nil
}