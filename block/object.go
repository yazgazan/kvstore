package block

import (
	"fmt"
	"io"
	"sync"

	"github.com/yazgazan/kvstore/container"
)

type ObjectMeta struct {
	chunk *container.Chunk

	Name       string
	StartBlock uint32
	Deleted    bool
}

type Object struct {
	db *BlockDB

	m           *sync.Mutex
	blocks      []*BlockMeta
	offset      int64
	posBlockIdx int
	posBlockOff uint32
}

type ObjectStats struct {
	Size   int64
	Blocks int
	Free   int // bytes available at the end of last block
}

func (o *Object) Stats() ObjectStats {
	o.m.Lock()
	defer o.m.Unlock()

	return o.stats()
}

func (o *Object) stats() ObjectStats {
	stats := ObjectStats{
		Blocks: len(o.blocks),
		Size:   o.size(),
	}

	lastBlock := o.blocks[len(o.blocks)-1]
	stats.Free = (int(o.db.meta.BlockSize) - int(o.db.sizeMeta)) - int(lastBlock.End)

	return stats
}

func (o *Object) Size() int64 {
	o.m.Lock()
	defer o.m.Unlock()

	return o.size()
}
func (o *Object) size() int64 {
	var size int64

	for _, b := range o.blocks {
		size += int64(b.End)
	}

	return size
}

func (o *Object) Read(p []byte) (int, error) {
	o.m.Lock()
	defer o.m.Unlock()
	o.db.m.Lock()
	defer o.db.m.Unlock()

	return o.read(p)
}

func (o *Object) read(p []byte) (int, error) {
	blockMeta := o.blocks[o.posBlockIdx]

	if o.posBlockOff == blockMeta.End {
		return 0, io.EOF
	}

	canRead := blockMeta.End - o.posBlockOff
	if int(canRead) > len(p) {
		canRead = uint32(len(p))
	}
	_, err := o.db.f.Seek(blockMeta.pos+int64(blockMeta.Size())+int64(o.posBlockOff), io.SeekStart)
	if err != nil {
		return 0, err
	}
	n, err := o.db.f.Read(p[:canRead])
	if err != nil {
		return n, err
	}

	o.offset += int64(n)
	o.posBlockOff += uint32(n)
	if o.posBlockOff == blockMeta.End && blockMeta.Next != 0 {
		o.posBlockIdx++
		o.posBlockOff = 0
	}
	p = p[n:]
	if len(p) == 0 {
		return n, nil
	}

	nnext, err := o.read(p)
	return n + nnext, err
}

func (o *Object) Write(p []byte) (int, error) {
	o.m.Lock()
	defer o.m.Unlock()
	o.db.m.Lock()
	defer o.db.m.Unlock()

	return o.write(p)
}

func (o *Object) write(p []byte) (int, error) {
	blockMeta := o.blocks[o.posBlockIdx]

	canWrite := (o.db.meta.BlockSize - uint32(blockMeta.Size())) - o.posBlockOff
	if int(canWrite) > len(p) {
		canWrite = uint32(len(p))
	}
	_, err := o.db.f.Seek(blockMeta.pos+int64(blockMeta.Size())+int64(o.posBlockOff), io.SeekStart)
	if err != nil {
		return 0, err
	}
	n, err := o.db.f.Write(p[:canWrite])
	if err != nil {
		return n, err
	}

	o.offset += int64(n)
	o.posBlockOff += uint32(n)
	if o.posBlockOff > blockMeta.End {
		blockMeta.End = o.posBlockOff
		err = blockMeta.WriteEnd(o.db.f)
		if err != nil {
			return n, err
		}
	}
	p = p[n:]
	if len(p) == 0 {
		return n, nil
	}
	if o.posBlockOff == o.db.meta.BlockSize-uint32(blockMeta.Size()) {
		if blockMeta.Next == 0 {
			newBlocks := uint32(len(p) / int(o.db.meta.BlockSize))
			if newBlocks == 0 {
				newBlocks = 1
			}
			err = o.alloc(newBlocks)
			if err != nil {
				return n, err
			}
		}
		o.posBlockIdx++
		o.posBlockOff = 0
	}

	nnext, err := o.write(p)
	return n + nnext, err
}

func (o *Object) alloc(n uint32) error {
	newFreeBlocks := make([]*BlockMeta, 0, n)
	next := o.db.meta.FirstFreeBlock
	for n > 0 && next != 0 {
		// Use free blocks
		newBlockMeta := &BlockMeta{
			pos: o.db.sizeMeta + int64(next)*int64(o.db.meta.BlockSize),
			idx: next,
		}
		_, err := o.db.f.Seek(newBlockMeta.pos, io.SeekStart)
		if err != nil {
			return err
		}
		_, err = newBlockMeta.ReadFrom(o.db.f)
		if err != nil {
			return err
		}
		next = newBlockMeta.Next
		newFreeBlocks = append(newFreeBlocks, newBlockMeta)
		n--
	}
	if len(newFreeBlocks) > 0 {
		o.db.meta.FirstFreeBlock = next
		err := o.db.meta.WriteFirstFreeBlock(o.db.f)
		if err != nil {
			return err
		}
		newFreeBlocks[len(newFreeBlocks)-1].Next = 0
		_, err = o.db.f.Seek(newFreeBlocks[len(newFreeBlocks)-1].pos, io.SeekStart)
		if err != nil {
			return err
		}
		err = newFreeBlocks[len(newFreeBlocks)-1].WriteNext(o.db.f)
		if err != nil {
			return err
		}
		o.blocks[len(o.blocks)-1].Next = newFreeBlocks[0].idx
		_, err = o.db.f.Seek(o.blocks[len(o.blocks)-1].pos, io.SeekStart)
		if err != nil {
			return err
		}
		err = o.blocks[len(o.blocks)-1].WriteNext(o.db.f)
		if err != nil {
			return err
		}
		o.blocks = append(o.blocks, newFreeBlocks...)
		if n == 0 {
			return nil
		}
	}

	blocks, err := o.db.grow(n, false)
	if err != nil {
		return err
	}
	if len(o.blocks) > 0 {
		o.blocks[len(o.blocks)-1].Next = blocks[0].idx
		err = o.blocks[len(o.blocks)-1].WriteNext(o.db.f)
		if err != nil {
			return err
		}
	}
	o.blocks = append(o.blocks, blocks...)

	return nil
}

func (o *Object) Seek(offset int64, whence int) (int64, error) {
	o.m.Lock()
	defer o.m.Unlock()

	return o.seek(offset, whence)
}

func (o *Object) seek(offset int64, whence int) (int64, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("unknown whence %d", whence)
	case 0:
		return o.seekFromStart(offset)
	case 1:
		return o.seekFromRelative(offset)
	case 2:
		return o.seekFromEnd(offset)
	}
}

func (o *Object) seekFromStart(offset int64) (int64, error) {
	o.offset = 0
	o.posBlockIdx = 0
	o.posBlockOff = 0

	return o.seekFromRelative(offset)
}

func (o *Object) seekFromEnd(offset int64) (int64, error) {
	var size int64
	for _, b := range o.blocks {
		size += int64(b.End)
	}
	o.offset = size
	o.posBlockIdx = len(o.blocks) - 1
	o.posBlockOff = o.blocks[o.posBlockIdx].End

	return o.seekFromRelativeBack(offset)
}

func (o *Object) seekFromRelative(offset int64) (int64, error) {
	if offset < 0 {
		return o.seekFromRelativeBack(-1 * offset)
	}

	blockMeta := o.blocks[o.posBlockIdx]

	if offset == 0 {
		return o.offset, nil
	}
	if o.posBlockOff == blockMeta.End {
		return 0, io.EOF
	}

	canRead := blockMeta.End - o.posBlockOff
	if int64(canRead) > offset {
		canRead = uint32(offset)
	}

	o.offset += int64(canRead)
	o.posBlockOff += canRead
	if o.posBlockOff == blockMeta.End && blockMeta.Next != 0 {
		o.posBlockIdx++
		o.posBlockOff = 0
	}
	offset -= int64(canRead)
	if offset == 0 {
		return o.offset, nil
	}

	return o.seekFromRelative(offset)
}

func (o *Object) seekFromRelativeBack(offset int64) (int64, error) {
	if offset == 0 {
		return o.offset, nil
	}
	if o.posBlockOff == 0 && o.posBlockIdx == 0 {
		return 0, io.EOF
	}

	canRead := o.posBlockOff
	if int64(canRead) > offset {
		canRead = uint32(offset)
	}

	o.offset -= int64(canRead)
	o.posBlockOff -= canRead
	if o.posBlockOff == 0 && o.posBlockIdx != 0 {
		o.posBlockIdx--
		o.posBlockOff = o.blocks[o.posBlockIdx].End
	}
	offset -= int64(canRead)
	if offset == 0 {
		return o.offset, nil
	}

	return o.seekFromRelativeBack(offset)
}
