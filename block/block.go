package block

import (
	"encoding/binary"
	"fmt"
	"io"
)

type BlockMeta struct {
	pos int64
	idx uint32

	End  uint32 // relative to blockStart + sizeof(blockMeta)
	Next uint32
}

var (
	sizeEnd  = binarySizePanic(BlockMeta{}.End)
	sizeNext = binarySizePanic(BlockMeta{}.Next)
)

func (m BlockMeta) Size() int {
	return sizeEnd + sizeNext
}

func (m BlockMeta) WriteTo(w io.Writer) (n int64, err error) {
	err = binary.Write(w, binary.LittleEndian, m.End)
	if err != nil {
		return n, fmt.Errorf("writing End offset: %w", err)
	}
	n += int64(sizeEnd)

	err = binary.Write(w, binary.LittleEndian, m.Next)
	if err != nil {
		return n, fmt.Errorf("writing next pointer: %w", err)
	}
	n += int64(sizeNext)

	return n, nil
}

func (m BlockMeta) WriteEnd(w io.WriteSeeker) error {
	_, err := w.Seek(m.pos, io.SeekStart)
	if err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, m.End)
}

func (m BlockMeta) WriteNext(w io.WriteSeeker) error {
	_, err := w.Seek(m.pos+int64(sizeEnd), io.SeekStart)
	if err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, m.Next)
}

func (m *BlockMeta) ReadFrom(r io.Reader) (n int64, err error) {
	err = binary.Read(r, binary.LittleEndian, &m.End)
	if err != nil {
		return n, fmt.Errorf("reading End offset: %w", err)
	}
	n += int64(sizeEnd)

	err = binary.Read(r, binary.LittleEndian, &m.Next)
	if err != nil {
		return n, fmt.Errorf("reading next pointer: %w", err)
	}
	n += int64(sizeNext)

	return n, nil
}
