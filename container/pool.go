package container

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

type Pool struct {
	m          *sync.RWMutex
	f          io.ReadWriteSeeker
	chunks     map[int64]*Chunk
	freeChunks map[int64]*Chunk
}

func NewPool(f io.ReadWriteSeeker) (*Pool, error) {
	pool := &Pool{
		m:          &sync.RWMutex{},
		f:          f,
		chunks:     map[int64]*Chunk{},
		freeChunks: map[int64]*Chunk{},
	}

	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var pos int64
	for {
		chunk := &Chunk{
			pool: pool,
		}

		err = chunk.readHeaderFrom(f)
		if err == io.EOF {
			break
		}
		chunk.pos = pos
		pos, err = f.Seek(int64(chunk.cap), io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		pool.chunks[chunk.pos] = chunk
		if chunk.free {
			pool.freeChunks[chunk.pos] = chunk
		}
	}

	return pool, nil
}

func (p *Pool) Size() int {
	p.m.RLock()
	defer p.m.RUnlock()

	return len(p.chunks)
}

func (p *Pool) Allocated() []*Chunk {
	p.m.RLock()
	defer p.m.RUnlock()

	nn := make([]*Chunk, 0, len(p.chunks))
	for _, n := range p.chunks {
		if n.free {
			continue
		}

		nn = append(nn, n)
	}

	return nn
}

func (p *Pool) Alloc(n uint32) (*Chunk, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if len(p.freeChunks) > 0 {
		for idx, chunk := range p.freeChunks {
			if chunk.cap < n {
				continue
			}
			chunk.size = 0
			chunk.free = false
			err := chunk.writeHeader()
			if err != nil {
				return nil, err
			}

			delete(p.freeChunks, idx)
			return chunk, nil
		}
	}

	chunk := &Chunk{
		pool: p,

		cap: n,
	}
	err := chunk.initialize()
	if err != nil {
		return nil, err
	}

	p.chunks[chunk.pos] = chunk

	return chunk, nil
}

func (p *Pool) AllocAndWrite(b []byte) (*Chunk, error) {
	chunk, err := p.Alloc(uint32(len(b)))
	if err != nil {
		return nil, err
	}
	_, err = chunk.Write(b)
	if err != nil {
		_ = chunk.Free()

		return nil, err
	}

	return chunk, err
}

func (p *Pool) Get(ptr ChunkPtr) (*Chunk, error) {
	p.m.RLock()
	defer p.m.RUnlock()

	chunk, ok := p.chunks[int64(ptr)]
	if !ok {
		return nil, fmt.Errorf("chunk not found at 0x%x", ptr)
	}

	return chunk, nil
}

type ChunkPtr int64

type Chunk struct {
	pool *Pool
	pos  int64

	cap  uint32
	size uint32
	free bool
}

var (
	sizeCap  = binarySizePanic(Chunk{}.cap)
	sizeSize = binarySizePanic(Chunk{}.size)
	sizeFree = binarySizePanic(Chunk{}.free)
)

func (Chunk) headerSize() int {
	return sizeCap + sizeSize + sizeFree
}

func (c Chunk) Ptr() ChunkPtr {
	return ChunkPtr(c.pos)
}

func (c *Chunk) Free() error {
	c.pool.m.Lock()
	defer c.pool.m.Unlock()

	return c.freeChunk()
}

func (c *Chunk) freeChunk() error {
	c.free = true
	err := c.writeHeader()
	if err != nil {
		c.free = false
		return err
	}

	c.pool.freeChunks[c.pos] = c

	return nil
}

func (c *Chunk) initialize() error {
	fileEnd, err := c.pool.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	c.pos = fileEnd

	err = c.writeHeaderTo(c.pool.f)
	if err != nil {
		return err
	}

	b := bytes.Repeat([]byte{0}, int(c.cap))
	_, err = c.pool.f.Write(b)

	return err
}

func (c *Chunk) readHeaderFrom(r io.Reader) error {
	err := binary.Read(r, binary.LittleEndian, &c.cap)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.LittleEndian, &c.size)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.LittleEndian, &c.free)
	if err != nil {
		return err
	}

	return nil
}

func (c *Chunk) writeHeader() error {
	_, err := c.pool.f.Seek(c.pos, io.SeekStart)
	if err != nil {
		return err
	}

	return c.writeHeaderTo(c.pool.f)
}

func (c *Chunk) writeHeaderTo(w io.Writer) error {
	err := binary.Write(w, binary.LittleEndian, c.cap)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, c.size)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, c.free)
	if err != nil {
		return err
	}

	return nil
}

func (c *Chunk) Write(p []byte) (n int, err error) {
	if len(p) > int(c.cap) {
		return 0, errors.New("chunk too small")
	}

	c.pool.m.Lock()
	defer c.pool.m.Unlock()

	c.size = uint32(len(p))
	err = c.writeHeader()
	if err != nil {
		return 0, err
	}

	return c.pool.f.Write(p)
}

func (c *Chunk) WriteAt(p []byte, off int64) (n int, err error) {
	if int(off)+len(p) > int(c.cap) {
		return 0, errors.New("chunk too small")
	}

	c.pool.m.Lock()
	defer c.pool.m.Unlock()

	if int(off)+len(p) > int(c.size) {
		c.size = uint32(len(p)) + uint32(off)
		err = c.writeHeader()
		if err != nil {
			return 0, err
		}
	}

	_, err = c.pool.f.Seek(c.pos+int64(c.headerSize())+off, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return c.pool.f.Write(p)
}

func (c *Chunk) Read(p []byte) (n int, err error) {
	if len(p) > int(c.size) {
		p = p[:c.size]
	}

	c.pool.m.RLock()
	defer c.pool.m.RUnlock()

	_, err = c.pool.f.Seek(c.pos+int64(c.headerSize()), io.SeekStart)
	if err != nil {
		return 0, err
	}

	return c.pool.f.Read(p)
}

func (c *Chunk) ReadAll() ([]byte, error) {
	b := make([]byte, c.size)

	_, err := c.Read(b)

	return b, err
}

func (c *Chunk) Size() uint32 {
	c.pool.m.RLock()
	defer c.pool.m.RUnlock()

	return c.size
}

func (c *Chunk) Cap() uint32 {
	c.pool.m.RLock()
	defer c.pool.m.RUnlock()

	return c.cap
}
