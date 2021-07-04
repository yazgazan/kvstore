package block_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/yazgazan/kvstore/block"
)

var tmpDirPath string

func TestMain(m *testing.M) {
	var (
		code int
		err  error
	)

	tmpDirPath, err = os.MkdirTemp(os.TempDir(), "kvstore-test-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create temporary folder: %v\n", err)
		os.Exit(1)
	}
	clean := func() {
		err = os.RemoveAll(tmpDirPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to remove temporary folder: %v\n", err)
			os.Exit(1)
		}
	}
	defer clean()

	code = m.Run()
	clean()
	os.Exit(code)
}

func TestBlockDB(t *testing.T) {
	fpath := filepath.Join(tmpDirPath, "test-block-db")
	t.Logf("test file: %q", fpath)
	failNowIfFailed := func() {
		if t.Failed() {
			t.FailNow()
		}
	}

	t.Run("Create", func(t *testing.T) {
		f, err := os.Create(fpath)
		if err != nil {
			t.Errorf("unexpected error creating file: %v", err)
			return
		}
		defer func() {
			errClose := f.Close()
			if errClose != nil {
				t.Errorf("unexpected error closing file: %v", errClose)
			}
		}()

		_, err = block.Create(f)
		if err != nil {
			t.Errorf("unexpected error creating block DB: %v", err)
			return
		}
	})
	failNowIfFailed()

	t.Run("Open", func(t *testing.T) {
		f, err := os.Open(fpath)
		if err != nil {
			t.Errorf("unexpected error opening object: %v", err)
			return
		}
		defer func() {
			errClose := f.Close()
			if errClose != nil {
				t.Errorf("unexpected error closing file: %v", errClose)
			}
		}()

		_, err = block.Open(f)
		if err != nil {
			t.Errorf("unexpected error opening block DB: %v", err)
			return
		}
	})
	failNowIfFailed()

	open := func() (db *block.BlockDB, close func()) {
		t.Helper()
		f, err := os.OpenFile(fpath, os.O_RDWR, 0600)
		if err != nil {
			t.Errorf("unexpected error opening object: %v", err)
			return nil, func() {}
		}
		close = func() {
			errClose := f.Close()
			if errClose != nil {
				t.Errorf("unexpected error closing file: %v", errClose)
			}
		}

		db, err = block.Open(f)
		if err != nil {
			t.Errorf("unexpected error opening block DB: %v", err)
			return nil, close
		}

		return db, close
	}
	subTest := func(name string, fn func(t *testing.T, db *block.BlockDB)) {
		t.Helper()
		db, closer := open()
		defer closer()
		if db == nil {
			return
		}

		t.Run(name, func(t *testing.T) {
			fn(t, db)
		})
		failNowIfFailed()
	}

	const growBy = 4
	subTest("Grow", func(t *testing.T, db *block.BlockDB) {
		err := db.Grow(growBy)
		if err != nil {
			t.Errorf("unexpected error growing block DB: %v", err)
			return
		}
	})

	subTest("FileSize", func(t *testing.T, db *block.BlockDB) {
		size, err := db.FileSize()
		if err != nil {
			t.Errorf("unexpected error getting file size: %v", err)
			return
		}
		meta := db.Meta()
		var expected = int64(meta.Size()) + (1+growBy)*int64(meta.BlockSize)
		if size != expected {
			t.Errorf("unexpected file size %d, expected %d", size, expected)
			return
		}
	})

	subTest("Blocks", func(t *testing.T, db *block.BlockDB) {
		t.Logf("meta: %#v\n", db.Meta())
		bb, err := db.Blocks()
		if err != nil {
			t.Errorf("unexpected error reading block headers: %v", err)
			return
		}

		for i, b := range bb {
			t.Logf("block %d: %#v\n", i, b)
		}

		for _, o := range db.Objects() {
			t.Logf("object %q: %#v\n", o.Name, o)
		}
	})

	const testString = "hello, world!"
	subTest("Create", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Create("foo")
		if err != nil {
			t.Errorf("unexpected error creating object: %v", err)
			return
		}

		b := []byte(testString)
		n, err := obj.Write(b)
		if err != nil {
			t.Errorf("unexpected error writing to object: %v", err)
			return
		}
		if n != len(b) {
			t.Errorf("obj.Write(%q) = %d, expected %d", b, n, len(b))
			return
		}
	})

	subTest("Open", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Open("foo")
		if err != nil {
			t.Errorf("unexpected error opening object: %v", err)
			return
		}

		b, err := io.ReadAll(obj)
		if err != nil {
			t.Errorf("unexpected error reading object: %v", err)
			return
		}
		got := string(b)
		if got != testString {
			t.Errorf("io.ReadAll(obj) = %q, expected %q", got, testString)
			return
		}
	})

	subTest("Blocks", func(t *testing.T, db *block.BlockDB) {
		t.Logf("meta: %#v\n", db.Meta())
		bb, err := db.Blocks()
		if err != nil {
			t.Errorf("unexpected error reading block headers: %v", err)
			return
		}

		for i, b := range bb {
			t.Logf("block %d: %#v\n", i, b)
		}

		for _, o := range db.Objects() {
			t.Logf("object %q: %#v\n", o.Name, o)
		}
	})

	const testN = 25000
	subTest("Write", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Create("bar")
		if err != nil {
			t.Errorf("unexpected error opening object: %v", err)
			return
		}

		b := bytes.Repeat([]byte{'A'}, testN)
		n, err := obj.Write(b)
		if err != nil {
			t.Errorf("unexpected error writing to object: %v", err)
			return
		}
		if n != testN {
			t.Errorf("obj.Write(...) = %d, expected %d", n, testN)
			return
		}
	})

	subTest("Read", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Open("bar")
		if err != nil {
			t.Errorf("unexpected error opening object: %v", err)
			return
		}

		b, err := io.ReadAll(obj)
		if err != nil {
			t.Errorf("unexpected error readding object: %v", err)
			return
		}
		if len(b) != testN {
			t.Errorf("len(io.ReadAll(...)) = %d, expected %d", len(b), testN)
			return
		}
	})

	subTest("Blocks", func(t *testing.T, db *block.BlockDB) {
		t.Logf("meta: %#v\n", db.Meta())
		bb, err := db.Blocks()
		if err != nil {
			t.Errorf("unexpected error reading block headers: %v", err)
			return
		}

		for i, b := range bb {
			t.Logf("block %d: %#v\n", i, b)
		}

		for _, o := range db.Objects() {
			t.Logf("object %q: %#v\n", o.Name, o)
		}
	})

	subTest("Size", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Open("bar")
		if err != nil {
			t.Errorf("unexpected error opening object: %v", err)
			return
		}

		got := obj.Size()

		if got != testN {
			t.Errorf("obj.Size() = %d, expected %d", got, testN)
			return
		}
	})

	subTest("Seek", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Create("baz")
		if err != nil {
			t.Errorf("unexpected error creating object: %v", err)
			return
		}

		alphabet := "abcdefghijklmnopqrstuvwxyz"
		n, err := obj.Write([]byte(alphabet))
		if err != nil {
			t.Errorf("obj.Write(%q) = %d, expected %d", alphabet, n, len(alphabet))
			return
		}

		got, err := obj.Seek(0, io.SeekStart)
		if err != nil {
			t.Errorf("obj.Seek(0, start): unexpected error: %v", err)
			return
		}
		if got != 0 {
			t.Errorf("obj.Seek(0, start) = %d, expected %d", got, 0)
			return
		}

		s, err := readStringN(obj, 3)
		if err != nil {
			t.Errorf("obj.Read(3): unexpected error: %v", err)
			return
		}
		if s != alphabet[:3] {
			t.Errorf("obj.Read(3) = %q, expected %q", s, alphabet[:3])
			return
		}

		got, err = obj.Seek(5, io.SeekEnd)
		if err != nil {
			t.Errorf("obj.Seek(5, end): unexpected error: %v", err)
			return
		}
		if got != int64(len(alphabet))-5 {
			t.Errorf("obj.Seek(5, end) = %d, expected %d", got, len(alphabet))
			return
		}

		s, err = readStringN(obj, 4)
		if err != nil {
			t.Errorf("obj.Read(4): unexpected error: %v", err)
			return
		}
		if s != alphabet[21:25] {
			t.Errorf("obj.Read(4) = %q, expected %q", s, alphabet[21:25])
			return
		}

		got, err = obj.Seek(-6, io.SeekCurrent)
		if err != nil {
			t.Errorf("obj.Seek(-6, current): unexpected error: %v", err)
			return
		}
		if got != 19 {
			t.Errorf("obj.Seek(-6, current) = %d, expected %d", got, 19)
			return
		}

		got, err = obj.Seek(4, io.SeekCurrent)
		if err != nil {
			t.Errorf("obj.Seek(4, current): unexpected error: %v", err)
			return
		}
		if got != 23 {
			t.Errorf("obj.Seek(4, current) = %d, expected %d", got, 23)
			return
		}

		_, err = obj.Seek(1, io.SeekEnd)
		if err != nil {
			t.Errorf("obj.Seek(1, end): unexpected error: %v", err)
			return
		}

		_, err = obj.Seek(3, io.SeekCurrent)
		if err != io.EOF {
			t.Errorf("obj.Seek(3, current): expected EOF, got %v", err)
			return
		}

		_, err = obj.Seek(2, io.SeekStart)
		if err != nil {
			t.Errorf("obj.Seek(2, start): unexpected error: %v", err)
			return
		}

		_, err = obj.Seek(-3, io.SeekCurrent)
		if err != io.EOF {
			t.Errorf("obj.Seek(-3, current): expected EOF, got %v", err)
			return
		}
	})

	subTest("Seek big object", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Open("bar")
		if err != nil {
			t.Errorf("unexpected error opening object: %v", err)
			return
		}

		got, err := obj.Seek(9000, io.SeekStart)
		if err != nil {
			t.Errorf("obj.Seek(9000, start): unexpected error: %v", err)
			return
		}
		if got != 9000 {
			t.Errorf("obj.Seek(9000, start) = %d, expected 9000", got)
			return
		}

		got, err = obj.Seek(-5000, io.SeekCurrent)
		if err != nil {
			t.Errorf("obj.Seek(-5000, current): unexpected error: %v", err)
			return
		}
		if got != 4000 {
			t.Errorf("obj.Seek(-5000, current) = %d, expected 4000", got)
			return
		}
	})

	var (
		freeBlocks uint32
		objects    uint32
	)
	subTest("db Stats", func(t *testing.T, db *block.BlockDB) {
		stats, err := db.Stats()
		if err != nil {
			t.Errorf("db.Stats(): unexpected error: %v", err)
			return
		}

		t.Logf("db stats: %#v\n", stats)
		freeBlocks = stats.FreeBlocks
		objects = stats.Objects
	})

	subTest("ReCreate", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Create("bar")
		if err != nil {
			t.Errorf("unexpected error creating object: %v", err)
			return
		}

		size := obj.Size()
		if size != 0 {
			t.Errorf("obj.Size() = %d, expected %d", size, 0)
			return
		}

		stats, err := db.Stats()
		if err != nil {
			t.Errorf("db.Stats(): unexpected error: %v", err)
			return
		}
		newFreeBlocks := stats.FreeBlocks - freeBlocks
		expected := uint32(math.Ceil(float64(testN)/float64(stats.DBMeta.BlockSize))) - 1 // 1 block is always retained
		if newFreeBlocks != expected {
			t.Errorf("expected %d more blocks to be freed, found %d", expected, newFreeBlocks)
			return
		}
		freeBlocks = stats.FreeBlocks
	})

	subTest("Delete", func(t *testing.T, db *block.BlockDB) {
		err := db.Delete("bar")
		if err != nil {
			t.Errorf("db.Delete(): unexpected error: %v", err)
		}

		stats, err := db.Stats()
		if err != nil {
			t.Errorf("db.Stats(): unexpected error: %v", err)
			return
		}

		if stats.Objects != objects-1 {
			t.Errorf("db.Stats().Objects = %d, expected %d", stats.Objects, objects-1)
			return
		}
		freeBlocks = stats.FreeBlocks
	})

	subTest("Grow again", func(t *testing.T, db *block.BlockDB) {
		_, err := db.Stats()
		if err != nil {
			t.Errorf("db.Stats(): unexpected error: %v", err)
			return
		}

		err = db.Grow(2)
		if err != nil {
			t.Errorf("db.Grow(2): unexpected error: %v", err)
			return
		}

		stats, err := db.Stats()
		if err != nil {
			t.Errorf("db.Stats(): unexpected error: %v", err)
			return
		}

		if stats.FreeBlocks != freeBlocks+2 {
			t.Errorf("db.Stats().FreeBlocks() = %d, expected %d", stats.FreeBlocks, freeBlocks+2)
			return
		}
	})

	subTest("UseAllFreeBlocks", func(t *testing.T, db *block.BlockDB) {
		obj, err := db.Create("UseAllFreeBlocks")
		if err != nil {
			t.Errorf("unexpected error creating object: %v", err)
			return
		}

		dbStats, err := db.Stats()
		if err != nil {
			t.Errorf("db.Stats(): unexpected error: %v", err)
			return
		}
		freeBlocks := dbStats.FreeBlocks
		blocks := dbStats.DBMeta.BlockCount

		objStats := obj.Stats()
		b := bytes.Repeat([]byte{'z'}, objStats.Free)
		n, err := obj.Write(b)
		if err != nil {
			t.Errorf("obj.Write(%d): unexpected error: %v", len(b), err)
			return
		}
		if n != len(b) {
			t.Errorf("obj.Write(%d) = %d, expected %d", len(b), n, len(b))
			return
		}

		objStats = obj.Stats()
		if objStats.Free != 0 {
			t.Errorf("obj.Stats().Free = %d, expected %d", objStats.Free, 0)
			return
		}

		dbStats, err = db.Stats()
		if err != nil {
			t.Errorf("db.Stats(): unexpected error: %v", err)
			return
		}
		if dbStats.FreeBlocks != freeBlocks {
			t.Errorf("db.Stats().FreeBlocks = %d, expected %d", dbStats.FreeBlocks, freeBlocks)
			return
		}
		if dbStats.DBMeta.BlockCount != blocks {
			t.Errorf("db.Stats().DBMeta.BlockCount = %d, expected %d", dbStats.DBMeta.BlockCount, blocks)
			return
		}

		b = bytes.Repeat([]byte{'x'}, int(dbStats.FreeBlocks)*(int(dbStats.DBMeta.BlockSize)-dbStats.BlockMetaSize))
		n, err = obj.Write(b)
		if err != nil {
			t.Errorf("obj.Write(%d): unexpected error: %v", len(b), err)
			return
		}
		if n != len(b) {
			t.Errorf("obj.Write(%d) = %d, expected %d", len(b), n, len(b))
			return
		}

		objStats = obj.Stats()
		if objStats.Free != 0 {
			t.Errorf("obj.Stats().Free = %d, expected %d", objStats.Free, 0)
			return
		}

		dbStats, err = db.Stats()
		if err != nil {
			t.Errorf("db.Stats(): unexpected error: %v", err)
			return
		}
		if dbStats.FreeBlocks != 0 {
			t.Errorf("db.Stats().FreeBlocks = %d, expected %d", dbStats.FreeBlocks, 0)
			return
		}
		if dbStats.DBMeta.BlockCount != blocks {
			t.Errorf("db.Stats().DBMeta.BlockCount = %d, expected %d", dbStats.DBMeta.BlockCount, blocks)
			return
		}
	})
}

func readStringN(r io.Reader, n int) (string, error) {
	p := make([]byte, n)
	rn, err := r.Read(p)
	p = p[:rn]

	return string(p), err
}
