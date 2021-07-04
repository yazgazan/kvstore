package container

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type KVNode struct {
	pool  *Pool
	chunk *Chunk

	prev  ChunkPtr
	next  ChunkPtr
	key   ChunkPtr
	value ChunkPtr
}

type kvnodeDTO struct {
	Prev  ChunkPtr
	Next  ChunkPtr
	Key   ChunkPtr
	Value ChunkPtr
}

var sizeKVNode = binarySizePanic(kvnodeDTO{})

func (n KVNode) dto() kvnodeDTO {
	return kvnodeDTO{
		Prev:  n.prev,
		Next:  n.next,
		Key:   n.key,
		Value: n.value,
	}
}

func NewKVNode(pool *Pool, key, value ChunkPtr) (*KVNode, error) {
	chunk, err := pool.Alloc(uint32(sizeKVNode))
	if err != nil {
		return nil, err
	}

	node := &KVNode{
		pool:  pool,
		chunk: chunk,

		key:   key,
		value: value,
	}

	err = node.Write()

	return node, err
}

func NewKVNodeFromChunk(pool *Pool, chunk *Chunk) (*KVNode, error) {
	node := &KVNode{
		pool:  pool,
		chunk: chunk,
	}

	err := node.Read()
	return node, err
}

func NewKVNodeFromChunkPtr(pool *Pool, chunkPtr ChunkPtr) (*KVNode, error) {
	chunk, err := pool.Get(chunkPtr)
	if err != nil {
		return nil, err
	}

	return NewKVNodeFromChunk(pool, chunk)
}

func (n *KVNode) Ptr() ChunkPtr {
	if n == nil {
		return 0
	}

	return n.chunk.Ptr()
}

func (n *KVNode) Write() error {
	buf := bytes.NewBuffer(make([]byte, 0, sizeKVNode))

	err := binary.Write(buf, binary.LittleEndian, n.dto())
	if err != nil {
		return err
	}

	if n.chunk == nil {
		n.chunk, err = n.pool.Alloc(uint32(sizeKVNode))
		if err != nil {
			return err
		}

		return n.Write()
	}

	_, err = n.chunk.Write(buf.Bytes())

	return err
}

func (n *KVNode) Read() error {
	b, err := n.chunk.ReadAll()
	if err != nil {
		return err
	}

	var dto kvnodeDTO

	buf := bytes.NewBuffer(b)
	err = binary.Read(buf, binary.LittleEndian, &dto)
	if err != nil {
		return err
	}

	n.prev = dto.Prev
	n.next = dto.Next
	n.key = dto.Key
	n.value = dto.Value

	return nil
}

func (n *KVNode) Key() (*Chunk, error) {
	return n.pool.Get(n.key)
}

func (n *KVNode) KeyBytes() ([]byte, error) {
	chunk, err := n.pool.Get(n.key)
	if err != nil {
		return nil, err
	}

	return chunk.ReadAll()
}

func (n *KVNode) Value() (*Chunk, error) {
	return n.pool.Get(n.value)
}

func (n *KVNode) ValueBytes() ([]byte, error) {
	chunk, err := n.pool.Get(n.value)
	if err != nil {
		return nil, err
	}

	return chunk.ReadAll()
}

func (n *KVNode) SetValue(value ChunkPtr) (old ChunkPtr, err error) {
	oldValue, err := n.Value()
	if err != nil {
		return 0, err
	}

	n.value = value
	err = n.Write()
	if err != nil {
		return 0, err
	}

	return oldValue.Ptr(), nil
}

func (n *KVNode) Next() (*KVNode, error) {
	if n.next == 0 {
		return nil, nil
	}

	chunk, err := n.pool.Get(n.next)
	if err != nil {
		return nil, err
	}

	node := &KVNode{
		pool:  n.pool,
		chunk: chunk,
	}

	err = node.Read()

	return node, err
}

func (n *KVNode) Prev() (*KVNode, error) {
	if n.prev == 0 {
		return nil, nil
	}

	chunk, err := n.pool.Get(n.prev)
	if err != nil {
		return nil, err
	}

	node := &KVNode{
		pool:  n.pool,
		chunk: chunk,
	}

	err = node.Read()

	return node, err
}

func (n *KVNode) ListSize() (int64, error) {
	var err error

	if n == nil {
		return 0, nil
	}

	node := n

	var size int64 = 1
	for node.next != 0 {
		node, err = node.Next()
		if err != nil {
			return size, err
		}
		size++
	}

	return size, nil
}

func (n *KVNode) Append(key, value ChunkPtr) (*KVNode, error) {
	var err error

	lastNode := n
	for lastNode.next != 0 {
		lastNode, err = lastNode.Next()
		if err != nil {
			return nil, err
		}
	}

	chunk, err := n.pool.Alloc(uint32(sizeKVNode))
	if err != nil {
		return nil, err
	}

	node := &KVNode{
		pool:  n.pool,
		chunk: chunk,

		prev:  lastNode.chunk.Ptr(),
		key:   key,
		value: value,
	}

	err = node.Write()
	if err != nil {
		return nil, err
	}

	lastNode.next = node.chunk.Ptr()
	err = lastNode.Write()

	return node, err
}

func (n *KVNode) Delete() (newHead ChunkPtr, err error) {
	if n.prev == 0 {
		err = n.chunk.Free()
		if err != nil {
			return n.Ptr(), err
		}
		next, err := n.Next()
		if err != nil || next == nil {
			return n.next, err
		}
		next.prev = 0
		err = next.Write()

		return n.next, err
	}

	head := n
	for head.prev != 0 {
		head, err = head.Prev()
		if err != nil {
			return 0, err
		}
	}

	prev, err := n.Prev()

	prev.next = n.next
	err = prev.Write()
	if err != nil {
		return 0, err
	}

	err = n.chunk.Free()

	return head.chunk.Ptr(), err
}

func (n *KVNode) DeleteAll() error {
	if n.prev != 0 {
		return errors.New(".DeleteAll() can only be called on the head node")
	}

	for {
		newHead, err := n.Delete()
		if err != nil {
			return err
		}
		if newHead == 0 {
			break
		}
		n, err = NewKVNodeFromChunkPtr(n.pool, newHead)
		if err != nil {
			return err
		}
	}

	return nil
}
