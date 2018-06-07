package rocksdb

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/rubenv/gorocksdb"
	"github.com/tiglabs/baudengine/util/bytes"
)

var _ store.KVWriter = &Writer{}

type Writer struct {
	db *gorocksdb.DB
	mo store.MergeOperator
	wOps *gorocksdb.WriteOptions
	rOps *gorocksdb.ReadOptions
}

func NewWriter(db *gorocksdb.DB, mo store.MergeOperator) *Writer {
	return &Writer{db: db, mo: mo, wOps: gorocksdb.NewDefaultWriteOptions(), rOps: gorocksdb.NewDefaultReadOptions()}
}

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.mo)
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) (err error) {
	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	_batch := gorocksdb.NewWriteBatch()
	// defer function to ensure that once started,
	// we either Commit tx or Rollback
	defer func() {
		// if nothing went wrong, commit
		if err == nil {
			// careful to catch error here too
			err = w.db.Write(w.wOps, _batch)
		}
		_batch.Destroy()
	}()

	for k, mergeOps := range emulatedBatch.Merger.Merges {
		var existingVal []byte
		kb := []byte(k)
		existingVal, err = w.get(kb)
		if err != nil {
			return
		}
		mergedVal, fullMergeOk := w.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			err = fmt.Errorf("merge operator returned failure")
			return
		}
		_batch.Put(kb, mergedVal)
	}

	for _, op := range emulatedBatch.Ops {
		if op.V != nil {
			_batch.Put(op.K, op.V)
		} else {
			_batch.Delete(op.K)
		}
	}
	return
}

func (w *Writer) get(key []byte) ([]byte, error) {
	v, err := w.db.Get(w.rOps, key)
	if err != nil {
		return nil, err
	}
	defer v.Free()
	if v.Size() == 0 {
		return nil, nil
	}
	return bytes.CloneBytes(v.Data()), nil
}

func (w *Writer) Close() error {
	w.wOps.Destroy()
	w.rOps.Destroy()
	return nil
}