package container

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

type hashBucket struct {
	Type bucketType
	Head ChunkPtr

	pool  *Pool
	chunk *Chunk
	idx   int
}

var (
	sizeType        = binarySizePanic(hashBucket{}.Type)
	sizeHead        = binarySizePanic(hashBucket{}.Head)
	sizeHashBucket  = sizeType + sizeHead
	sizeHashBuckets = HashMapN * sizeHashBucket
)

type bucketType uint8

const (
	bucketTypeList bucketType = iota
	bucketTypeBuckets
)

type hashBuckets [HashMapN]*hashBucket

func newHashBuckets(pool *Pool, chunk *Chunk) hashBuckets {
	var hh hashBuckets

	for i := range hh {
		hh[i] = &hashBucket{
			pool:  pool,
			chunk: chunk,
			idx:   i,
		}
	}

	return hh
}

func (bb hashBuckets) WriteTo(chunk *Chunk) error {
	buf := bytes.NewBuffer(make([]byte, 0, sizeHashBuckets))

	for _, bucket := range bb {
		_ = binary.Write(buf, binary.LittleEndian, bucket.Type)
		_ = binary.Write(buf, binary.LittleEndian, bucket.Head)
	}

	_, err := chunk.Write(buf.Bytes())

	return err
}

func (bb *hashBuckets) ReadFrom(chunk *Chunk) error {
	b, err := chunk.ReadAll()
	if err != nil {
		return err
	}
	if len(b) != sizeHashBuckets {
		return fmt.Errorf("expected to read %d bytes, read %d", sizeHashBuckets, len(b))
	}

	buf := bytes.NewBuffer(b)

	for _, bucket := range bb {
		_ = binary.Read(buf, binary.LittleEndian, &bucket.Type)
		_ = binary.Read(buf, binary.LittleEndian, &bucket.Head)
	}

	return nil
}

func (bb hashBuckets) bucket(key []byte) *hashBucket {
	salt := []byte(strconv.FormatInt(bb[0].chunk.pos, 32))
	h := hashKey(append(salt, key...))

	return bb[h.Sum32()%HashMapN]
}

func (bb hashBuckets) findBucket(key []byte) (*hashBucket, error) {
	bucket := bb.bucket(key)

	switch bucket.Type {
	default:
		return nil, fmt.Errorf("invalid bucket type %v", bucket.Type)
	case bucketTypeBuckets:
		chunk, err := bb[0].pool.Get(bucket.Head)
		if err != nil {
			return nil, err
		}
		newBuckets := newHashBuckets(chunk.pool, chunk)
		err = newBuckets.ReadFrom(chunk)
		if err != nil {
			return nil, err
		}

		return newBuckets.findBucket(key)
	case bucketTypeList:
		return bucket, nil
	}
}

func (bb hashBuckets) Upsert(key []byte, value ChunkPtr) error {
	bucket, err := bb.findBucket(key)
	if err != nil {
		return err
	}

	return bucket.Upsert(key, value)
}

func (b *hashBucket) Write() error {
	buf := bytes.NewBuffer(make([]byte, 0, sizeHashBucket))

	_ = binary.Write(buf, binary.LittleEndian, &b.Type)
	_ = binary.Write(buf, binary.LittleEndian, &b.Head)

	_, err := b.chunk.WriteAt(buf.Bytes(), int64(b.idx)*int64(sizeHashBucket))

	return err
}

var errorBucketFull = errors.New("bucket full")

func (b *hashBucket) Upsert(key []byte, value ChunkPtr) error {
	if b.Head == 0 {
		keyChunk, err := b.pool.AllocAndWrite(key)
		if err != nil {
			return err
		}

		return b.Append(key, keyChunk.Ptr(), value)
	}

	node, err := b.findHashMapItem(key)
	if err != nil {
		return err
	}
	if node == nil {
		keyChunk, err := b.pool.AllocAndWrite(key)
		if err != nil {
			return err
		}
		return b.Append(key, keyChunk.Ptr(), value)
	}

	old, err := node.SetValue(value)
	if err != nil {
		return err
	}
	oldChunk, err := b.pool.Get(old)
	if err != nil {
		return err
	}

	return oldChunk.Free()
}

func (b *hashBucket) Append(keyBytes []byte, key, value ChunkPtr) error {
	if b.Type != bucketTypeList {
		return fmt.Errorf("cannot append to bucket of type %v", b.Type)
	}

	if b.Head == 0 {
		head, err := NewKVNode(b.pool, key, value)
		if err != nil {
			return err
		}

		b.Head = head.Ptr()

		return b.Write()
	}

	head, err := NewKVNodeFromChunkPtr(b.pool, b.Head)
	if err != nil {
		return err
	}

	size, err := head.ListSize()
	if err != nil {
		return err
	}
	if size < HashMapMaxList {
		_, err = head.Append(key, value)

		return err
	}

	chunk, err := b.pool.Alloc(uint32(sizeHashBuckets))
	if err != nil {
		return err
	}
	newBuckets := newHashBuckets(b.pool, chunk)
	err = newBuckets.WriteTo(chunk)
	if err != nil {
		return err
	}

	node := head
	for node != nil {
		nodeKey, err := node.KeyBytes()
		if err != nil {
			return err
		}
		bucket, err := newBuckets.findBucket(nodeKey)
		if err != nil {
			return err
		}

		err = bucket.Append(nodeKey, node.key, node.value)
		if err != nil {
			return err
		}

		node, err = node.Next()
		if err != nil {
			return err
		}
	}

	bucket, err := newBuckets.findBucket(keyBytes)
	if err != nil {
		return err
	}

	err = bucket.Append(keyBytes, key, value)
	if err != nil {
		return err
	}

	b.Type = bucketTypeBuckets
	b.Head = chunk.Ptr()
	err = b.Write()
	if err != nil {
		return err
	}

	return head.DeleteAll()
}

func (b *hashBucket) findHashMapItem(needle []byte) (*KVNode, error) {
	head, err := NewKVNodeFromChunkPtr(b.pool, b.Head)
	if err != nil {
		return nil, err
	}

	return findHashMapItem(head, needle)
}

func findHashMapItem(n *KVNode, needle []byte) (*KVNode, error) {
	if n == nil {
		return nil, nil
	}

	for n != nil {
		key, err := n.KeyBytes()
		if err != nil {
			return nil, err
		}

		if bytes.Equal(key, needle) {
			return n, nil
		}

		n, err = n.Next()
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}
