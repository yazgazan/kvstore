package block

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/yazgazan/kvstore/container"
)

const (
	Magic            = 1978942581
	LatestVersion    = 1
	DefaultBlockSize = 4096
)

var MinimumBlockSize = uint32(BlockMeta{}.Size() + 1)

type BlockDB struct {
	m *sync.Mutex

	f io.ReadWriteSeeker

	meta     DBMeta
	sizeMeta int64
	objects  map[string]*ObjectMeta
	indexObj *Object
	index    *container.Pool
}

func Create(f io.ReadWriteSeeker, opts ...Option) (*BlockDB, error) {
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if off != 0 {
		return nil, fmt.Errorf("expected empty file, found %d bytes", off)
	}

	meta := DBMeta{
		Magic:   Magic,
		Version: LatestVersion,

		BlockSize: DefaultBlockSize,
	}
	for _, opt := range opts {
		opt(&meta)
	}
	if meta.BlockSize < MinimumBlockSize {
		return nil, fmt.Errorf("invalid block size %d (should be greater or equal to %d)", meta.BlockSize, MinimumBlockSize)
	}

	sizeMeta, err := meta.WriteTo(f)
	if err != nil {
		return nil, fmt.Errorf("failed to write meta: %w", err)
	}

	db := &BlockDB{
		m: &sync.Mutex{},

		f: f,

		meta:     meta,
		sizeMeta: sizeMeta,
		objects:  map[string]*ObjectMeta{},
	}

	db.indexObj = &Object{
		db: db,

		m: &sync.Mutex{},
		blocks: []*BlockMeta{
			{
				pos: db.sizeMeta,
				idx: 0,
			},
		},
	}

	db.index, err = container.NewPool(db.indexObj)
	if err != nil {
		return nil, err
	}

	_, err = db.grow(1, false)

	return db, err
}

func Open(f io.ReadWriteSeeker) (*BlockDB, error) {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var meta DBMeta

	sizeMeta, err := meta.ReadFrom(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta: %w", err)
	}

	db := &BlockDB{
		m: &sync.Mutex{},

		f: f,

		meta:     meta,
		sizeMeta: sizeMeta,
		objects:  map[string]*ObjectMeta{},
	}

	indexBlocks, err := db.blocks(0)
	if err != nil {
		return nil, err
	}

	db.indexObj = &Object{
		db: db,

		m:      &sync.Mutex{},
		blocks: indexBlocks,
	}

	db.index, err = container.NewPool(db.indexObj)
	if err != nil {
		return nil, err
	}

	chunks := db.index.Allocated()
	for _, chunk := range chunks {
		objMeta := &ObjectMeta{
			chunk: chunk,
		}

		b, err := chunk.ReadAll()
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(b, objMeta)
		if err != nil {
			return nil, err
		}

		db.objects[objMeta.Name] = objMeta
	}

	return db, err
}

func (db *BlockDB) free(idx uint32) error {
	meta := BlockMeta{
		pos: db.sizeMeta + int64(idx)*int64(db.meta.BlockSize),
		idx: idx,
	}
	_, err := db.f.Seek(meta.pos, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = meta.ReadFrom(db.f)
	if err != nil {
		return err
	}

	next := meta.Next

	meta.Next = db.meta.FirstFreeBlock
	meta.End = 0
	_, err = db.f.Seek(meta.pos, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = meta.WriteTo(db.f)
	if err != nil {
		return err
	}

	db.meta.FirstFreeBlock = meta.idx
	err = db.meta.WriteFirstFreeBlock(db.f)
	if err != nil {
		return err
	}

	if next != 0 {
		return db.free(next)
	}

	return nil
}

func (db *BlockDB) allocSingle() (*BlockMeta, error) {
	if db.meta.FirstFreeBlock != 0 {
		meta := &BlockMeta{
			pos: db.sizeMeta + int64(db.meta.FirstFreeBlock)*int64(db.meta.BlockSize),
			idx: db.meta.FirstFreeBlock,
		}
		_, err := db.f.Seek(meta.pos, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = meta.ReadFrom(db.f)
		if err != nil {
			return nil, err
		}

		db.meta.FirstFreeBlock = meta.Next
		err = db.meta.WriteFirstFreeBlock(db.f)
		if err != nil {
			return nil, err
		}

		meta.Next = 0
		_, err = db.f.Seek(meta.pos, io.SeekStart)
		if err != nil {
			return nil, err
		}
		err = meta.WriteNext(db.f)
		if err != nil {
			return nil, err
		}

		return meta, nil
	}

	mm, err := db.grow(1, false)
	if err != nil {
		return nil, err
	}

	return mm[0], nil
}

func (db *BlockDB) Grow(n uint32) error {
	db.m.Lock()
	defer db.m.Unlock()

	_, err := db.grow(n, true)

	return err
}

func (db *BlockDB) grow(n uint32, free bool) ([]*BlockMeta, error) {
	startNewBlocks := db.sizeMeta + int64(db.meta.BlockCount)*int64(db.meta.BlockSize)
	_, err := db.f.Seek(startNewBlocks, io.SeekStart)
	if err != nil {
		return nil, err
	}

	b := bytes.Repeat([]byte{0}, int(n*db.meta.BlockSize))
	_, err = db.f.Write(b)
	if err != nil {
		return nil, err
	}

	mm := make([]*BlockMeta, n)
	for i := uint32(0); i < n; i++ {
		blockPos := startNewBlocks + int64(i)*int64(db.meta.BlockSize)
		_, err = db.f.Seek(blockPos, io.SeekStart)
		if err != nil {
			return nil, err
		}

		meta := &BlockMeta{
			pos:  blockPos,
			idx:  db.meta.BlockCount + i,
			Next: db.meta.BlockCount + i + 1,
		}
		if i == n-1 {
			meta.Next = 0
		}
		mm[i] = meta

		_, err = meta.WriteTo(db.f)
		if err != nil {
			return nil, err
		}
	}

	db.meta.BlockCount += n

	err = db.meta.WriteBlockCount(db.f)
	if err != nil {
		return nil, err
	}

	if !free {
		return mm, nil
	}

	if db.meta.FirstFreeBlock == 0 {
		db.meta.FirstFreeBlock = db.meta.BlockCount - n
		err = db.meta.WriteFirstFreeBlock(db.f)
		if err != nil {
			return nil, err
		}
	} else {
		firstFreeBlock, err := db.findLastFreeBlock()
		if err != nil {
			return nil, err
		}
		meta := BlockMeta{
			pos: db.sizeMeta + int64(firstFreeBlock)*int64(db.meta.BlockSize),
			idx: firstFreeBlock,

			Next: mm[0].idx,
		}
		_, err = db.f.Seek(meta.pos, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = meta.WriteTo(db.f)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (db *BlockDB) findLastFreeBlock() (uint32, error) {
	start := db.meta.FirstFreeBlock

	// start of first free block
	curPos, err := db.f.Seek(int64(db.sizeMeta)+int64(start)*int64(db.meta.BlockSize), io.SeekStart)
	if err != nil {
		return 0, err
	}
	blockMeta := BlockMeta{}
	_, err = blockMeta.ReadFrom(db.f)
	if err != nil {
		return 0, err
	}

	for blockMeta.Next != 0 {
		curPos, err = db.f.Seek(int64(db.sizeMeta)+int64(blockMeta.Next)*int64(db.meta.BlockSize), io.SeekStart)
		if err != nil {
			return 0, err
		}
		_, err = blockMeta.ReadFrom(db.f)
		if err != nil {
			return 0, err
		}
	}

	lastFreeBlock := (curPos - int64(db.sizeMeta)) / int64(db.meta.BlockSize)
	if lastFreeBlock == 0 {
		return 0, errors.New("last free block cannot be at position 0")
	}

	return uint32(lastFreeBlock), nil
}

func (db *BlockDB) FileSize() (int64, error) {
	db.m.Lock()
	defer db.m.Unlock()

	return db.f.Seek(0, io.SeekEnd)
}

func (db *BlockDB) Meta() DBMeta {
	db.m.Lock()
	defer db.m.Unlock()

	return db.meta
}

func (db *BlockDB) Blocks() ([]BlockMeta, error) {
	db.m.Lock()
	defer db.m.Unlock()

	curPos, err := db.f.Seek(int64(db.sizeMeta), io.SeekStart)
	if err != nil {
		return nil, err
	}

	mm := make([]BlockMeta, db.meta.BlockCount)
	for i := uint32(0); i < db.meta.BlockCount; i++ {
		meta := BlockMeta{
			pos: curPos,
			idx: i,
		}
		n, err := meta.ReadFrom(db.f)
		if err != nil {
			return nil, err
		}
		mm[i] = meta

		curPos, err = db.f.Seek(int64(db.meta.BlockSize)-n, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
	}

	return mm, nil
}

func (db *BlockDB) blocks(start uint32) ([]*BlockMeta, error) {
	block := &BlockMeta{
		pos: db.sizeMeta + int64(start)*int64(db.meta.BlockSize),
		idx: start,
	}
	_, err := db.f.Seek(block.pos, io.SeekStart)
	if err != nil {
		return nil, err
	}
	_, err = block.ReadFrom(db.f)
	if err != nil {
		return nil, err
	}
	bb := []*BlockMeta{
		block,
	}

	for block.Next != 0 {
		block = &BlockMeta{
			pos: db.sizeMeta + int64(block.Next)*int64(db.meta.BlockSize),
			idx: block.Next,
		}
		_, err = db.f.Seek(block.pos, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = block.ReadFrom(db.f)
		if err != nil {
			return nil, err
		}

		bb = append(bb, block)
	}

	return bb, nil
}
