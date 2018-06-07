package rocksdb

import (
	"os"
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/rubenv/gorocksdb"
)

const (
	Name                    = "rocksdb"
)

var _ store.KVStore = &Store{}

type Store struct {
	path string
	db   *gorocksdb.DB
	mo   store.MergeOperator
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	if path == "" {
		return nil, os.ErrInvalid
	}

    ops := gorocksdb.NewDefaultOptions()
    ops.SetCreateIfMissing(true)
    var db *gorocksdb.DB
    var err error
    if config["read_only"].(bool) {
    	db, err = gorocksdb.OpenDbForReadOnly(ops, path, true)
    } else {
	    db, err = gorocksdb.OpenDb(ops, path)
    }
	if err != nil {
		return nil, err
	}
	rv := Store{
		path: path,
		db:   db,
		mo: mo,
	}
	return &rv, nil
}


func (s *Store) Writer() (store.KVWriter, error) {
	return NewWriter(s.db, s.mo), nil
}

// Reader returns a KVReader which can be used to
// read data from the KVStore.  If a reader cannot
// be obtained a non-nil error is returned.
func(s *Store) Reader() (store.KVReader, error) {
	return NewReader(s.db), nil
}

func (bs *Store) Close() error {
	if bs == nil {
		return nil
	}
	if bs.db != nil {
		bs.db.Close()
	}
	return nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
