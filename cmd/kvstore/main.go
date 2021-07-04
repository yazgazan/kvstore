package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/yazgazan/kvstore"
)

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <path> <get|list|set|delete|buckets> [command options...]\n", flag.CommandLine.Name())
		os.Exit(2)
	}
	fpath := args[0]
	args = args[1:]
	store, err := kvstore.NewFromFile(fpath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	cmd := (args[0])
	args = args[1:]

	switch strings.ToLower(cmd) {
	default:
		fmt.Fprintf(os.Stderr, "Error: unknown command %q\n", cmd)
		os.Exit(2)
	case "get":
		if len(args) != 2 {
			fmt.Fprintf(os.Stderr, "Usage: %s <path> get <bucket> <key>\n", flag.CommandLine.Name())
			os.Exit(2)
		}
		err = Get(store, args[0], args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "list":
		if len(args) != 1 {
			fmt.Fprintf(os.Stderr, "Usage: %s <path> list <bucket>\n", flag.CommandLine.Name())
			os.Exit(2)
		}
		err = List(store, args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "set":
		if len(args) != 3 {
			fmt.Fprintf(os.Stderr, "Usage: %s <path> set <bucket> <key> <value>\n", flag.CommandLine.Name())
			os.Exit(2)
		}
		err = Set(store, args[0], args[1], args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "delete":
		if len(args) != 2 {
			fmt.Fprintf(os.Stderr, "Usage: %s <path> delete <bucket> <key>\n", flag.CommandLine.Name())
			os.Exit(2)
		}
		err = Delete(store, args[0], args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "buckets":
		err = Buckets(store)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}
}

func Buckets(store kvstore.Store) error {
	buckets, err := store.Buckets()
	if err != nil {
		return err
	}

	sort.Strings(buckets)
	for _, b := range buckets {
		fmt.Printf("%q\n", b)
	}

	return nil
}

func List(store kvstore.Store, bucket string) error {
	tx := store.Reader()
	defer tx.Rollback()

	keys, err := tx.List(bucket)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	sort.Strings(keys)
	for _, k := range keys {
		fmt.Println(k)
	}

	return nil
}

func Get(store kvstore.Store, bucket, key string) error {
	var b json.RawMessage

	err := store.Get(bucket, key, &b)
	if err != nil {
		return err
	}
	os.Stdout.Write(b)

	return nil
}

func Set(store kvstore.Store, bucket, key, value string) error {
	tx := store.Writer()
	defer tx.Rollback()

	err := tx.Set(bucket, key, value)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func Delete(store kvstore.Store, bucket, key string) error {
	tx := store.Writer()
	defer tx.Rollback()

	err := tx.Delete(bucket, key)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return err
}
