package container

import (
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"sync"
)

const (
	HashMapN       = 128
	HashMapMaxList = 32
)

type HashMap struct {
	m *sync.RWMutex

	pool             *Pool
	headBuckets      hashBuckets
	headBucketsChunk *Chunk
}

func NewHashMap(f io.ReadWriteSeeker) (*HashMap, error) {
	pool, err := NewPool(f)
	if err != nil {
		return nil, err
	}

	m := &HashMap{
		m: &sync.RWMutex{},

		pool: pool,
	}

	if pool.Size() == 0 {
		m.headBucketsChunk, err = pool.Alloc(uint32(sizeHashBuckets))
		if err != nil {
			return nil, err
		}
		m.headBuckets = newHashBuckets(m.pool, m.headBucketsChunk)

		err = m.headBuckets.WriteTo(m.headBucketsChunk)

		return m, err
	}

	m.headBucketsChunk, err = pool.Get(0)
	if err != nil {
		return nil, err
	}
	m.headBuckets = newHashBuckets(m.pool, m.headBucketsChunk)

	err = m.headBuckets.ReadFrom(m.headBucketsChunk)

	return m, err
}

func hashKey(b []byte) hash.Hash32 {
	h := fnv.New32a()

	_, _ = h.Write(b)

	return h
}

func (m *HashMap) Delete(key []byte) error {
	m.m.Lock()
	defer m.m.Unlock()

	return m.delete(key)
}

func (m *HashMap) delete(key []byte) error {
	bucket, err := m.headBuckets.findBucket(key)
	if err != nil {
		return err
	}
	if bucket.Head == 0 {
		return fmt.Errorf("key %q not found", key)
	}

	head, err := NewKVNodeFromChunkPtr(m.pool, bucket.Head)
	if err != nil {
		return err
	}
	node, err := findHashMapItem(head, key)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("key %q not found", key)
	}

	newHead, err := node.Delete()
	if err != nil {
		return err
	}

	bucket.Head = newHead
	err = bucket.Write()

	return err
}

func (m *HashMap) Load(key []byte) ([]byte, bool, error) {
	m.m.RLock()
	defer m.m.RUnlock()

	return m.load(key)
}

func (m *HashMap) load(key []byte) ([]byte, bool, error) {
	bucket, err := m.headBuckets.findBucket(key)
	if err != nil {
		return nil, false, err
	}

	var head *KVNode
	if bucket.Head != 0 {
		head, err = NewKVNodeFromChunkPtr(m.pool, bucket.Head)
		if err != nil {
			return nil, false, err
		}
	}
	node, err := findHashMapItem(head, key)
	if err != nil {
		return nil, false, err
	}
	if node == nil {
		return nil, false, nil
	}

	b, err := node.ValueBytes()
	if err != nil {
		return nil, true, err
	}

	return b, true, nil
}

func (m *HashMap) Store(key, value []byte) error {
	m.m.Lock()
	defer m.m.Unlock()

	valueChunk, err := m.pool.AllocAndWrite(value)
	if err != nil {
		return err
	}

	return m.store(m.headBuckets, key, valueChunk)
}

func (m *HashMap) store(bb hashBuckets, key []byte, value *Chunk) error {
	return m.headBuckets.Upsert(key, value.Ptr())
}

func (m *HashMap) Range(f func(key, value []byte) bool) error {
	m.m.RLock()
	defer m.m.RUnlock()

	return m.rangeKeyValues(f)
}

func (m *HashMap) rangeKeyValues(f func(key, value []byte) bool) error {
	var itErr error
	err := m.iterateBuckets(func(_ int, _ hashBuckets, b *hashBucket) bool {
		if b.Type != bucketTypeList || b.Head == 0 {
			return true
		}
		node, err := NewKVNodeFromChunkPtr(m.pool, b.Head)
		if err != nil {
			itErr = err
			return false
		}

		for node != nil {
			key, err := node.KeyBytes()
			if err != nil {
				itErr = err
				return false
			}
			value, err := node.ValueBytes()
			if err != nil {
				itErr = err
				return false
			}

			ok := f(key, value)
			if !ok {
				return false
			}

			node, err = node.Next()
			if err != nil {
				itErr = err
				return false
			}
		}

		return true
	})
	if err != nil {
		return err
	}
	if itErr != nil {
		return itErr
	}

	return nil
}

type HashMapStats struct {
	PoolSize int
	MaxLoad  float64
	MaxDepth int
}

func (m *HashMap) Stats() (HashMapStats, error) {
	m.m.RLock()
	m.m.RUnlock()
	stats := HashMapStats{
		PoolSize: m.pool.Size(),
	}

	var itErr error
	counts := map[ChunkPtr]float64{}
	err := m.iterateBuckets(func(depth int, bb hashBuckets, b *hashBucket) bool {
		if depth > stats.MaxDepth {
			stats.MaxDepth = depth
		}
		if b.Type != bucketTypeList || b.Head == 0 {
			return true
		}
		head, err := NewKVNodeFromChunkPtr(m.pool, b.Head)
		if err != nil {
			itErr = err
			return false
		}
		size, err := head.ListSize()
		if err != nil {
			itErr = err
			return false
		}
		counts[bb[0].chunk.Ptr()] += float64(size)

		return true
	})
	if err != nil {
		return stats, err
	}
	if itErr != nil {
		return stats, err
	}

	for _, c := range counts {
		load := c / float64(HashMapN)
		if load > stats.MaxLoad {
			stats.MaxLoad = load
		}
	}

	return stats, nil
}

func (m *HashMap) iterateBuckets(f func(depth int, bb hashBuckets, b *hashBucket) bool) error {
	_, err := m.iterateBuckets2(m.headBuckets, 0, f)

	return err
}

func (m *HashMap) iterateBuckets2(head hashBuckets, depth int, f func(depth int, bb hashBuckets, b *hashBucket) bool) (bool, error) {
	for _, b := range head {
		ok := f(depth+1, head, b)
		if !ok {
			return false, nil
		}
		if b.Type != bucketTypeBuckets {
			continue
		}

		chunk, err := m.pool.Get(b.Head)
		if err != nil {
			return false, err
		}
		bb := newHashBuckets(m.pool, chunk)

		err = bb.ReadFrom(chunk)
		if err != nil {
			return false, err
		}

		ok, err = m.iterateBuckets2(bb, depth+1, f)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}
