package container_test

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/yazgazan/kvstore/container"
)

func TestPool(t *testing.T) {
	buf := newReadWriteSeeker(nil)

	pool, err := container.NewPool(buf)
	if err != nil {
		t.Errorf("NewPool(nil): unexpected error: %v", err)
		return
	}

	chunks := pool.Allocated()
	if len(chunks) != 0 {
		t.Errorf("len(NewPool(nil).Allocated()) = %d, expected 0", len(chunks))
		return
	}

	payload := jsonMustMarshal("foo")
	chunk, err := pool.Alloc(uint32(len(payload)))
	if err != nil {
		t.Errorf("pool.Alloc(%d): unexpected error: %v", len(payload), err)
		return
	}
	n, err := chunk.Write(payload)
	if err != nil {
		t.Errorf("chunk.Write(...): unexpected error: %v", err)
		return
	}
	if n != len(payload) {
		t.Errorf("chunk.Write(...) = %d, expected %d", n, len(payload))
		return
	}

	b := make([]byte, chunk.Size())
	n, err = chunk.Read(b)
	if err != nil {
		t.Errorf("chunk.Read(): unexpected error: %v", err)
		return
	}
	if n != len(b) {
		t.Errorf("chunk.Read() = %d, expected %d", n, len(b))
		return
	}
	if !bytes.Equal(b, payload) {
		t.Errorf("chunk.Read() = %q, expected %q", b, payload)
		return
	}

	pool, err = container.NewPool(buf)
	if err != nil {
		t.Errorf("NewPool(...): unexpected error: %v", err)
		return
	}

	chunks = pool.Allocated()
	if len(chunks) != 1 {
		t.Errorf("len(pool.Nodes()) = %d, expected %d", len(chunks), 1)
		return
	}
	chunk = chunks[0]

	b = make([]byte, chunk.Size())
	n, err = chunk.Read(b)
	if err != nil {
		t.Errorf("chunk.Read(): unexpected error: %v", err)
		return
	}
	if n != len(b) {
		t.Errorf("chunk.Read() = %d, expected %d", n, len(b))
		return
	}
	if !bytes.Equal(b, payload) {
		t.Errorf("chunk.Read() = %q, expected %q", b, payload)
		return
	}
}

func jsonMustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return b
}

type readWriteSeeker struct {
	pos int
	b   []byte
}

func newReadWriteSeeker(b []byte) io.ReadWriteSeeker {
	return &readWriteSeeker{
		b: b,
	}
}

func (buf *readWriteSeeker) Read(p []byte) (n int, err error) {
	r := bytes.NewReader(buf.b)
	n, err = r.ReadAt(p, int64(buf.pos))
	buf.pos += n

	return n, err
}

func (buf *readWriteSeeker) Write(p []byte) (n int, err error) {
	canCopy := len(p)
	if buf.pos+canCopy > len(buf.b) {
		canCopy = len(buf.b) - buf.pos
	}
	copy(buf.b[buf.pos:buf.pos+canCopy], p[:canCopy])
	buf.b = append(buf.b, p[canCopy:]...)
	buf.pos += len(p)

	return len(p), nil
}

func (buf *readWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	r := bytes.NewReader(buf.b)
	_, err := r.Seek(int64(buf.pos), io.SeekStart)
	if err != nil {
		return 0, err
	}

	n, err := r.Seek(offset, whence)
	if err != nil {
		return n, err
	}

	buf.pos = int(n)

	return n, nil
}
