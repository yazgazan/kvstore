package container_test

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	"github.com/yazgazan/kvstore/container"
)

func TestHashMap(t *testing.T) {
	buf := newReadWriteSeeker(nil)

	m, err := container.NewHashMap(buf)
	if err != nil {
		t.Errorf("NewHashMap(nil): unexpected error: %v", err)
		return
	}

	testKey := []byte("foo")
	value := []byte("bar")
	err = m.Store(testKey, value)
	if err != nil {
		t.Errorf("m.Store(%q, %q): unexpected error: %v", testKey, value, err)
		return
	}

	stats, err := m.Stats()
	if err != nil {
		t.Errorf("m.Stats(): unexpected error: %v", err)
		return
	}
	t.Logf("map stats: %+v", stats)

	got, ok, err := m.Load(testKey)
	if err != nil {
		t.Errorf("m.Load(%q): unexpected error %v", testKey, err)
		return
	}
	if !ok {
		t.Errorf("m.Load(%q): not found", testKey)
		return
	}
	if string(got) != string(value) {
		t.Errorf("m.Load(%q) = %q, expected %q", testKey, got, value)
		return
	}

	const (
		N              = 10000
		minKeyLength   = 3
		maxKeyLength   = 256
		minValueLength = 0
		maxValueLength = 1024 * 16
	)

	type testValue struct {
		Key, Value []byte
	}
	testValues := make([]testValue, N)
	findValue := func(values []testValue, needle []byte) (testValue, bool) {
		for _, value := range values {
			if bytes.Equal(value.Key, needle) {
				return value, true
			}
		}

		return testValue{}, false
	}

	t.Run("generate test values", func(t *testing.T) {

		testValueExists := func(values []testValue, needle []byte) bool {
			for _, value := range values {
				if bytes.Equal(value.Key, needle) {
					return true
				}
			}

			return false
		}
		var generateUniqueKey func(values []testValue) ([]byte, error)
		generateUniqueKey = func(values []testValue) ([]byte, error) {
			keyLength := minKeyLength + rand.Intn(maxKeyLength-minKeyLength)
			key := make([]byte, keyLength)
			_, err = rand.Read(key)
			if err != nil {
				return nil, err
			}
			if testValueExists(values, key) {
				return generateUniqueKey(values)
			}

			return key, nil
		}

		for i := range testValues {
			valueLength := minValueLength + rand.Intn(maxValueLength-minValueLength)
			value := make([]byte, valueLength)
			_, err = rand.Read(value)
			if err != nil {
				t.Errorf("rand.Read(%d): unexpected error: %v", len(value), err)
				return
			}
			key, err := generateUniqueKey(testValues)
			if err != nil {
				t.Errorf("generateUniqueKey(...): %v", err)
				return
			}

			testValues[i] = testValue{
				Key:   key,
				Value: value,
			}
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("Store/Load", func(t *testing.T) {
		for _, v := range testValues {
			err = m.Store(v.Key, v.Value)
			if err != nil {
				t.Errorf("m.Store(...): unexpected error: %v", err)
				return
			}

			got, ok, err := m.Load(v.Key)
			if err != nil {
				t.Errorf("m.Load(...): unexpected error: %v", err)
				return
			}
			if !ok {
				t.Error("m.Load(...): not found")
				return
			}
			if !bytes.Equal(got, v.Value) {
				t.Error("m.Load(...): value differs")
				return
			}
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	stats, err = m.Stats()
	if err != nil {
		t.Errorf("m.Stats(): unexpected error: %v", err)
		return
	}
	t.Logf("map stats: %+v", stats)

	t.Run("Load", func(t *testing.T) {
		for _, v := range testValues {
			got, ok, err := m.Load(v.Key)
			if err != nil {
				t.Errorf("m.Load(...): unexpected error: %v", err)
				continue
			}
			if !ok {
				t.Error("m.Load(...): key not found")
				continue
			}
			if !bytes.Equal(got, v.Value) {
				t.Error("m.Load(...): content not equal to expected")
				continue
			}
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("prepare new values", func(t *testing.T) {
		for i := range testValues {
			valueLength := minValueLength + rand.Intn(maxValueLength-minValueLength)
			value := make([]byte, valueLength)
			_, err = rand.Read(value)
			if err != nil {
				t.Errorf("rand.Read(%d): unexpected error: %v", len(value), err)
				return
			}

			testValues[i].Value = value
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("Store updated", func(t *testing.T) {
		for _, v := range testValues {
			err = m.Store(v.Key, v.Value)
			if err != nil {
				t.Errorf("m.Store(...): unexpected error: %v", err)
				return
			}
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("Load updated", func(t *testing.T) {
		for _, v := range testValues {
			got, ok, err := m.Load(v.Key)
			if err != nil {
				t.Errorf("m.Load(...): unexpected error: %v", err)
				return
			}
			if !ok {
				t.Error("m.Load(...): key not found")
				return
			}
			if !bytes.Equal(got, v.Value) {
				t.Error("m.Load(...): content not equal to expected")
				return
			}
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("Delete", func(t *testing.T) {
		err := m.Delete(testKey)
		if err != nil {
			t.Errorf("m.Delete(%q): unexpected error: %v", testKey, err)
			return
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("Load deleted", func(t *testing.T) {
		_, ok, err := m.Load(testKey)
		if err != nil {
			t.Errorf("m.Load(%q): unexpected error: %v", testKey, err)
			return
		}
		if ok {
			t.Errorf("m.Load(%q) found a value, expected none", testKey)
			return
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("Range", func(t *testing.T) {
		n := 0
		err := m.Range(func(key, value []byte) bool {
			v, ok := findValue(testValues, key)
			if !ok {
				t.Error("unexpected key")
				return false
			}
			if !bytes.Equal(value, v.Value) {
				t.Error("value does not match")
				return false
			}

			n++
			return true
		})
		if err != nil {
			t.Errorf("m.Range(...): unexpected error: %v", err)
			return
		}
		if n != len(testValues) {
			t.Errorf("m.Range(): covered %d values, expected %d", n, len(testValues))
			return
		}
	})

	t.Run("loading existing map", func(t *testing.T) {
		m, err = container.NewHashMap(buf)
		if err != nil {
			t.Errorf("NewHashMap(nil): unexpected error: %v", err)
			return
		}
	})

	t.Run("Load from existing map", func(t *testing.T) {
		for _, v := range testValues {
			got, ok, err := m.Load(v.Key)
			if err != nil {
				t.Errorf("m.Load(...): unexpected error: %v", err)
				return
			}
			if !ok {
				t.Error("m.Load(...): key not found")
				return
			}
			if !bytes.Equal(got, v.Value) {
				t.Error("m.Load(...): content not equal to expected")
				return
			}
		}
	})
	if t.Failed() {
		t.FailNow()
	}
}

func BenchmarkHashMap(b *testing.B) {
	buildMap := func(b *testing.B) *container.HashMap {
		b.Helper()

		buf := newReadWriteSeeker(nil)

		m, err := container.NewHashMap(buf)
		if err != nil {
			b.Errorf("NewHashMap(nil): unexpected error: %v", err)
			b.FailNow()
		}

		return m
	}

	m := buildMap(b)
	testValue := []byte("hello, world!")
	var maxN int64
	b.Run("Store unique keys", func(b *testing.B) {
		var n int64
		for i := 0; i < b.N; i++ {
			key := []byte(strconv.FormatInt(n, 32))
			err := m.Store(key, testValue)
			if err != nil {
				b.Errorf("failed to store key")
				return
			}

			maxN = n
			n++
		}
	})

	b.Run("Load unique keys", func(b *testing.B) {
		var n int64
		for i := 0; i < b.N; i++ {
			k := n % maxN
			key := []byte(strconv.FormatInt(k, 32))
			_, ok, err := m.Load(key)
			if err != nil {
				b.Errorf("failed to load key: %v", err)
				return
			}
			if !ok {
				b.Errorf("key not found")
				return
			}

			n++
		}
	})

	m = buildMap(b)
	keysMap := map[int64]struct{}{}
	b.Run("Store non-unique keys", func(b *testing.B) {
		var n int64
		for i := 0; i < b.N; i++ {
			keysMap[n%42] = struct{}{}
			key := []byte(strconv.FormatInt(n%42, 32))
			err := m.Store(key, testValue)
			if err != nil {
				b.Errorf("failed to store key: %v", err)
				return
			}

			n++
		}
	})
	keys := make([]int64, 0, len(keysMap))
	for k := range keysMap {
		keys = append(keys, k)
	}

	b.Run("Load non-unique keys", func(b *testing.B) {
		var n int
		for i := 0; i < b.N; i++ {
			k := keys[n%len(keys)]
			key := []byte(strconv.FormatInt(k, 32))
			_, ok, err := m.Load(key)
			if err != nil {
				b.Errorf("failed to load key: %v", err)
				return
			}
			if !ok {
				b.Errorf("key not found")
				return
			}

			n++
		}
	})
}
