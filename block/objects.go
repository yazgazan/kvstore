package block

import (
	"encoding/json"
	"io"
	"os"
	"sync"
)

func (db *BlockDB) Objects() []ObjectMeta {
	db.m.Lock()
	defer db.m.Unlock()

	oo := make([]ObjectMeta, len(db.objects))
	for _, o := range db.objects {
		oo = append(oo, *o)
	}

	return oo
}

func (db *BlockDB) Create(name string) (*Object, error) {
	meta, ok := db.objects[name]
	if ok {
		db.m.Lock()
		defer db.m.Unlock()
		blockMeta := &BlockMeta{
			pos: db.sizeMeta + int64(meta.StartBlock)*int64(db.meta.BlockSize),
			idx: meta.StartBlock,
		}
		_, err := db.f.Seek(blockMeta.pos, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = blockMeta.ReadFrom(db.f)
		if err != nil {
			return nil, err
		}
		next := blockMeta.Next
		blockMeta.Next = 0
		blockMeta.End = 0
		_, err = db.f.Seek(blockMeta.pos, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = blockMeta.WriteTo(db.f)
		if err != nil {
			return nil, err
		}
		if next != 0 {
			err = db.free(next)
			if err != nil {
				return nil, err
			}
		}

		return &Object{
			db:     db,
			m:      &sync.Mutex{},
			blocks: []*BlockMeta{blockMeta},
		}, nil
	}

	db.m.Lock()
	block, err := db.allocSingle()
	if err != nil {
		db.m.Unlock()
		return nil, err
	}

	meta = &ObjectMeta{
		Name:       name,
		StartBlock: block.idx,
	}

	b, err := json.Marshal(meta)
	if err != nil {
		db.m.Unlock()
		return nil, err
	}

	db.m.Unlock()
	chunk, err := db.index.AllocAndWrite(b)
	if err != nil {
		return nil, err
	}

	db.m.Lock()
	defer db.m.Unlock()

	meta.chunk = chunk

	db.objects[name] = meta

	return &Object{
		db: db,

		m:      &sync.Mutex{},
		blocks: []*BlockMeta{block},
	}, nil
}

func (db *BlockDB) Delete(name string) error {
	db.m.Lock()

	meta, ok := db.objects[name]
	if !ok {
		db.m.Unlock()
		return os.ErrNotExist
	}

	blockMeta := BlockMeta{
		pos: db.sizeMeta + int64(meta.StartBlock)*int64(db.meta.BlockSize),
		idx: meta.StartBlock,
	}
	_, err := db.f.Seek(blockMeta.pos, io.SeekStart)
	if err != nil {
		db.m.Unlock()
		return err
	}
	_, err = blockMeta.ReadFrom(db.f)
	if err != nil {
		db.m.Unlock()
		return err
	}

	db.m.Unlock()
	err = meta.chunk.Free()
	if err != nil {
		return err
	}
	db.m.Lock()

	delete(db.objects, name)

	err = db.free(blockMeta.idx)
	db.m.Unlock()

	return err
}

func (db *BlockDB) Open(name string) (*Object, error) {
	db.m.Lock()
	defer db.m.Unlock()

	meta, ok := db.objects[name]
	if !ok {
		return nil, os.ErrNotExist
	}

	blocks, err := db.blocks(meta.StartBlock)
	if err != nil {
		return nil, err
	}

	return &Object{
		db: db,

		m:      &sync.Mutex{},
		blocks: blocks,
	}, nil
}
