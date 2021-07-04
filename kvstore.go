package kvstore

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"sync"

	"github.com/yazgazan/kvstore/block"
	"github.com/yazgazan/kvstore/container"
)

var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrKeyNotFound    = errors.New("key not found")
)

type Store interface {
	io.Closer

	Reader() ReadTx
	Writer() WriteTx
	Get(bucket, key string, dst interface{}) error
}

type Tx interface {
	Commit() error
	Rollback() error
}

type ReadTx interface {
	Tx

	Get(bucket, key string, dst interface{}) error
	List(bucket string) ([]string, error)
}

type WriteTx interface {
	Tx

	Get(bucket, key string, dst interface{}) error
	List(bucket string) ([]string, error)
	Set(bucket, key string, value interface{}) error
	Delete(bucket, key string) error
}

type store struct {
	db     *block.BlockDB
	closer io.Closer

	m          *sync.RWMutex
	buckets    map[string]*container.HashMap // map[bucketName]hashmap
	bucketsMap *container.HashMap
}

func New(f io.ReadWriteSeeker) (Store, error) {
	var db *block.BlockDB

	n, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		db, err = block.Create(f)
	} else {
		db, err = block.Open(f)
	}
	if err != nil {
		return nil, err
	}

	obj, err := db.Create("objects")
	if err != nil {
		return nil, err
	}
	bucketsMap, err := container.NewHashMap(obj)
	if err != nil {
		return nil, err
	}

	st := &store{
		db: db,

		m:          &sync.RWMutex{},
		buckets:    map[string]*container.HashMap{},
		bucketsMap: bucketsMap,
	}

	return st, nil
}

func NewFromFile(fpath string) (Store, error) {
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	st, err := New(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	st.(*store).closer = f

	return st, nil
}

func (st *store) Close() error {
	if st.closer == nil {
		return nil
	}

	return st.closer.Close()
}

func (st *store) Get(bucket, key string, dst interface{}) error {
	tx := st.Reader()
	defer tx.Rollback()

	err := tx.Get(bucket, key, dst)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (st *store) Reader() ReadTx {
	st.m.RLock()

	return &readTx{
		store: st,
	}
}

type readTx struct {
	store *store
}

func (rtx *readTx) Commit() error {
	if rtx.store != nil {
		rtx.store.m.RUnlock()
		rtx.store = nil

		return nil
	}

	return errors.New("transaction rolled back")
}

func (rtx *readTx) Rollback() error {
	if rtx.store != nil {
		rtx.store.m.RUnlock()
		rtx.store = nil
	}

	return nil
}

func (rtx *readTx) Get(bucket, key string, dst interface{}) error {
	p := bucketPath(bucket)
	m, ok := rtx.store.buckets[p]
	if !ok {
		return ErrBucketNotFound
	}
	b, ok, err := m.Load([]byte(key))
	if err != nil {
		return err
	}
	if !ok {
		return ErrKeyNotFound
	}

	return json.Unmarshal(b, dst)
}

func (rtx *readTx) List(bucket string) ([]string, error) {
	p := bucketPath(bucket)
	m, ok := rtx.store.buckets[p]
	if !ok {
		return nil, ErrBucketNotFound
	}

	keys := []string{}
	err := m.Range(func(key, _ []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (st *store) Writer() WriteTx {
	st.m.Lock()

	return &writeTx{
		store: st,

		m:           &sync.RWMutex{},
		writeCache:  map[string]map[string]json.RawMessage{},
		deleteCache: map[string]map[string]bool{},
	}
}

type writeTx struct {
	m *sync.RWMutex

	store       *store
	writeCache  map[string]map[string]json.RawMessage
	deleteCache map[string]map[string]bool
}

func (wtx *writeTx) Commit() error {
	wtx.m.Lock()
	if wtx.store == nil {
		wtx.m.Unlock()
		return errors.New("transaction rolled back")
	}
	defer wtx.m.Unlock()
	defer wtx.store.m.Unlock()

	for name, bucket := range wtx.writeCache {
		deletedCache := wtx.deleteCache[name]
		for k, v := range bucket {
			if deletedCache != nil && deletedCache[k] {
				continue
			}
			err := wtx.write(name, k, v)
			if err != nil {
				wtx.store = nil
				return err
			}
		}
	}

	for name, bucket := range wtx.deleteCache {
		for k, deleted := range bucket {
			if !deleted {
				continue
			}

			m, ok := wtx.store.buckets[name]
			if !ok {
				continue
			}
			err := m.Delete([]byte(k))
			if err != nil {
				wtx.store = nil
				return err
			}
		}
	}

	wtx.store = nil

	return nil
}

func (wtx *writeTx) write(bucket, key string, payload json.RawMessage) error {
	m, ok := wtx.store.buckets[bucket]
	if !ok {
		p := bucketPath(bucket)
		obj, err := wtx.store.db.Create(p)
		if err != nil {
			return err
		}
		m, err = container.NewHashMap(obj)
		if err != nil {
			return err
		}
		err = wtx.store.bucketsMap.Store([]byte(bucket), []byte(p))
		if err != nil {
			return err
		}

		wtx.store.buckets[bucket] = m
	}
	return m.Store([]byte(key), payload)
}

func (wtx *writeTx) Rollback() error {
	wtx.m.Lock()
	if wtx.store != nil {
		wtx.store.m.Unlock()
		wtx.store = nil
	}

	return nil
}

func (wtx *writeTx) Get(bucket, key string, dst interface{}) error {
	wtx.m.RLock()
	cachedDelete, ok := wtx.deleteCache[bucket]
	if ok {
		if cachedDelete[key] {
			wtx.m.RUnlock()
			return ErrKeyNotFound
		}
	}
	cachedBucket, ok := wtx.writeCache[bucket]
	if ok {
		cached, ok := cachedBucket[key]
		if ok {
			wtx.m.RUnlock()
			return json.Unmarshal(cached, dst)
		}
	}
	wtx.m.RUnlock()

	p := bucketPath(bucket)
	m, ok := wtx.store.buckets[p]
	if !ok {
		return ErrBucketNotFound
	}
	b, ok, err := m.Load([]byte(key))
	if err != nil {
		return err
	}
	if !ok {
		return ErrKeyNotFound
	}

	return json.Unmarshal(b, dst)
}

func (wtx *writeTx) List(bucket string) ([]string, error) {
	p := bucketPath(bucket)
	m, ok := wtx.store.buckets[p]
	if !ok {
		return nil, ErrBucketNotFound
	}

	var deletedKeys []string
	wtx.m.RLock()
	cachedDelete, ok := wtx.deleteCache[bucket]
	if ok {
		for k, deleted := range cachedDelete {
			if !deleted {
				continue
			}
			deletedKeys = append(deletedKeys, k)
		}
	}
	wtx.m.RUnlock()

	keys := []string{}
	err := m.Range(func(key, _ []byte) bool {
		if contains(deletedKeys, string(key)) {
			return true
		}

		keys = append(keys, string(key))
		return true
	})
	if err != nil {
		return nil, err
	}

	wtx.m.RLock()
	cachedBucket, ok := wtx.writeCache[bucket]
	if ok {
		for k := range cachedBucket {
			if contains(keys, k) || contains(deletedKeys, k) {
				continue
			}
			keys = append(keys, k)
		}
	}
	wtx.m.RUnlock()

	return keys, nil
}

func (wtx *writeTx) Set(bucket, key string, value interface{}) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}

	wtx.m.Lock()
	b, ok := wtx.writeCache[bucket]
	if !ok {
		b = map[string]json.RawMessage{}
		wtx.writeCache[bucket] = b
	}
	b[key] = payload
	d, ok := wtx.deleteCache[bucket]
	if ok {
		d[key] = false
	}
	wtx.m.Unlock()

	return nil
}

func (wtx *writeTx) Delete(bucket, key string) error {
	wtx.m.Lock()

	d, ok := wtx.deleteCache[bucket]
	if !ok {
		d = map[string]bool{}
		wtx.deleteCache[bucket] = d
	}
	d[key] = true
	wtx.m.Unlock()

	return nil
}

func contains(ss []string, needle string) bool {
	for _, s := range ss {
		if s == needle {
			return true
		}
	}

	return false
}

func bucketPath(name string) string {
	return path.Join("bucket", name)
}
