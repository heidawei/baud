package rocksdb

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store/test"
	"github.com/blevesearch/bleve/index/store"
)

func open(t *testing.T) store.KVStore {
	config := make(map[string]interface{})
	config["path"] = "test"
	config["read_only"] = false
	rv, err := New(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

func cleanup(t *testing.T, s store.KVStore) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = os.RemoveAll("test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestRocksDBKVCrud(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestRocksDBReaderIsolation(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestReaderIsolation(t, s)
}

func TestRocksDBReaderOwnsGetBytes(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestRocksDBWriterOwnsBytes(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestRocksDBPrefixIterator(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestRocksDBPrefixIteratorSeek(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestRocksDBRangeIterator(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

func TestRocksDBRangeIteratorSeek(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestRangeIteratorSeek(t, s)
}

