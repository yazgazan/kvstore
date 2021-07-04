package block

import "io"

type Stats struct {
	DBMeta DBMeta

	Objects          uint32
	IndexObjectStats ObjectStats
	FreeBlocks       uint32
	BlockMetaSize    int
}

func (db *BlockDB) Stats() (Stats, error) {
	var err error

	db.m.Lock()
	defer db.m.Unlock()

	stats := Stats{
		DBMeta:        db.meta,
		Objects:       uint32(len(db.objects)),
		BlockMetaSize: BlockMeta{}.Size(),
	}

	stats.IndexObjectStats = db.indexObj.Stats()

	stats.FreeBlocks, err = db.countFreeBlocks()
	if err != nil {
		return stats, err
	}

	return stats, nil
}

func (db *BlockDB) countFreeBlocks() (uint32, error) {
	if db.meta.FirstFreeBlock == 0 {
		return 0, nil
	}

	meta := BlockMeta{
		pos: db.sizeMeta + int64(db.meta.FirstFreeBlock)*int64(db.meta.BlockSize),
		idx: db.meta.FirstFreeBlock,
	}
	_, err := db.f.Seek(meta.pos, io.SeekStart)
	if err != nil {
		return 0, err
	}
	_, err = meta.ReadFrom(db.f)
	if err != nil {
		return 0, err
	}

	var count uint32 = 1

	for meta.Next != 0 {
		meta = BlockMeta{
			pos: db.sizeMeta + int64(meta.Next)*int64(db.meta.BlockSize),
			idx: meta.Next,
		}
		_, err = db.f.Seek(meta.pos, io.SeekStart)
		if err != nil {
			return 0, err
		}
		_, err = meta.ReadFrom(db.f)
		if err != nil {
			return 0, err
		}

		count++
	}

	return count, nil
}
