package bleve

import (
	"context"
	"path"
	"errors"

	"github.com/blevesearch/bleve"
	"github.com/tiglabs/baudengine/engine"
	"github.com/blevesearch/bleve/index/store"
	"github.com/tiglabs/baudengine/engine/bleve/badgerdb"
)

const Name = "bleve"

var RAFT_APPLY_ID = []byte("_raft_apply_id")

var _ engine.Engine = &Bleve{}

type Bleve struct {
	index bleve.Index
}

func New(cfg engine.EngineConfig) (engine.Engine, error) {

	docMappings, err := ParseSchema([]byte(cfg.Schema))
	if err != nil {
		return nil, err
	}
	if docMappings == nil {
		return nil, errors.New("invalid schema")
	}
	mapping := bleve.NewIndexMapping()
	mapping.DefaultMapping = docMappings[0]
    kvconfig := make(map[string]interface{})
    kvconfig["path"] = path.Join(cfg.Path, "baud.bleve", "data")
    kvconfig["sync"] = false
    kvconfig["read_only"] = cfg.ReadOnly
	index, err := bleve.NewUsing(path.Join(cfg.Path, "baud.bleve"), mapping, bleve.Config.DefaultIndexType, badgerdb.Name, kvconfig)
	//index, err := bleve.New(path.Join(cfg.Path, "baud.bleve"), mapping)
	if err != nil {
		return nil, err
	}
	return &Bleve{index: index}, nil
}

func(b *Bleve)NewWriteBatch() engine.Batch {
	return NewBatch(b.index)
}

func (b *Bleve)NewSnapshot() (engine.Snapshot, error) {
	_, store, err := b.index.Advanced()
	if err != nil {
		return nil, err
	}
	reader, err := store.Reader()
	if err != nil {
		return nil, err
	}
	return &Snapshot{reader: reader}, nil
}

func (b *Bleve)ApplySnapshot(ctx context.Context, iter engine.Iterator) error {
	_, _store, err := b.index.Advanced()
	if err != nil {
		return err
	}
	writer, err := _store.Writer()
	if err != nil {
		return err
	}
	var batch store.KVBatch
	var count int
	for iter.Valid() {
		if batch == nil {
			batch = store.NewEmulatedBatch(nil)
		}
		batch.Set(iter.Key(), iter.Value())
		count++
		if count % 100 == 0 {
			err = writer.ExecuteBatch(batch)
			if err != nil {
				return err
			}
			batch = nil
		}
	}
	if batch != nil {
		return writer.ExecuteBatch(batch)
	}
	return nil
}


