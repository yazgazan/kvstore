package block

import (
	"encoding/binary"
	"fmt"
	"io"
)

type DBMeta struct {
	Magic   uint32
	Version uint32

	BlockSize  uint32
	BlockCount uint32

	FirstFreeBlock uint32
}

var (
	sizeMagic          = binarySizePanic(DBMeta{}.Magic)
	sizeVersion        = binarySizePanic(DBMeta{}.Version)
	sizeBlockSize      = binarySizePanic(DBMeta{}.BlockSize)
	sizeBlockCount     = binarySizePanic(DBMeta{}.BlockCount)
	sizeFirstFreeBlock = binarySizePanic(DBMeta{}.FirstFreeBlock)
)

type Option func(m *DBMeta)

func WithBlockSize(blockSize uint32) Option {
	return func(m *DBMeta) {
		m.BlockSize = blockSize
	}
}

func (m DBMeta) Size() int {
	return sizeMagic + sizeVersion + sizeBlockSize + sizeBlockCount + sizeFirstFreeBlock
}

func (m DBMeta) WriteTo(w io.Writer) (n int64, err error) {
	err = binary.Write(w, binary.LittleEndian, m.Magic)
	if err != nil {
		return n, fmt.Errorf("writing magic: %w", err)
	}
	n += int64(sizeMagic)

	err = binary.Write(w, binary.LittleEndian, m.Version)
	if err != nil {
		return n, fmt.Errorf("writing version: %w", err)
	}
	n += int64(sizeVersion)

	err = binary.Write(w, binary.LittleEndian, m.BlockSize)
	if err != nil {
		return n, fmt.Errorf("writing block size: %w", err)
	}
	n += int64(sizeBlockSize)

	err = binary.Write(w, binary.LittleEndian, m.BlockCount)
	if err != nil {
		return n, fmt.Errorf("writing block count: %w", err)
	}
	n += int64(sizeBlockCount)

	err = binary.Write(w, binary.LittleEndian, m.FirstFreeBlock)
	if err != nil {
		return n, fmt.Errorf("writing first free block pointer: %w", err)
	}
	n += int64(sizeFirstFreeBlock)

	return n, nil
}

func (m DBMeta) WriteBlockCount(w io.WriteSeeker) error {
	off := sizeMagic + sizeVersion + sizeBlockSize

	_, err := w.Seek(int64(off), io.SeekStart)
	if err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, m.BlockCount)
}

func (m DBMeta) WriteFirstFreeBlock(w io.WriteSeeker) error {
	off := sizeMagic + sizeVersion + sizeBlockSize + sizeBlockSize

	_, err := w.Seek(int64(off), io.SeekStart)
	if err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, m.FirstFreeBlock)
}

func (m *DBMeta) ReadFrom(r io.Reader) (n int64, err error) {
	err = binary.Read(r, binary.LittleEndian, &m.Magic)
	if err != nil {
		return n, fmt.Errorf("reading magic: %w", err)
	}
	n += int64(sizeMagic)
	if m.Magic != Magic {
		return n, fmt.Errorf("magic doesn't match: expected 0x%x, found 0x%x", Magic, m.Magic)
	}

	err = binary.Read(r, binary.LittleEndian, &m.Version)
	if err != nil {
		return n, fmt.Errorf("reading version: %w", err)
	}
	n += int64(sizeVersion)
	if m.Version > LatestVersion || m.Version == 0 {
		return n, fmt.Errorf("unsupported version %d, latest supported version is %d", m.Version, LatestVersion)
	}

	err = binary.Read(r, binary.LittleEndian, &m.BlockSize)
	if err != nil {
		return n, fmt.Errorf("reading block size: %w", err)
	}
	n += int64(sizeBlockSize)
	if m.BlockSize < MinimumBlockSize {
		return n, fmt.Errorf("invalid block size %d (should be greater or equal to %d)", m.BlockSize, MinimumBlockSize)
	}

	err = binary.Read(r, binary.LittleEndian, &m.BlockCount)
	if err != nil {
		return n, fmt.Errorf("reading block count: %w", err)
	}
	n += int64(sizeBlockCount)

	err = binary.Read(r, binary.LittleEndian, &m.FirstFreeBlock)
	if err != nil {
		return n, fmt.Errorf("reading first free block pointer: %w", err)
	}
	n += int64(sizeFirstFreeBlock)

	return n, nil
}
