// Package slowpoke implements a low-level key/value store in pure Go.
// Keys stored in memory, Value stored on disk
// It uses locking for multiple readers and a single writer.
package slowpoke

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/recoilme/syncfile"
	"github.com/tidwall/btree"
)

var (
	debug     = true
	filesFile = ""
	dbs       = make(map[string]*DB)

	ErrKeyNotFound = errors.New("Error: key not found")
	ErrDbOpened    = errors.New("Error: db is opened")
	ErrDbNotOpen   = errors.New("Error: db not open")

	bufPool = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

const (
	FileMode = 0666
)

type DB struct {
	Btree *btree.BTree
	Fkey  *syncfile.SyncFile
	Fval  *syncfile.SyncFile
	Mux   *sync.RWMutex
}

type Cmd struct {
	Type uint8
	Key  []byte
	Seek uint32
	Size uint32
}

func log(i interface{}) {
	if !debug {
		return
	}
	t := time.Now()
	fmt.Printf("%02d.%02d.%04d %02d:%02d:%02d\t%v\n",
		t.Day(), t.Month(), t.Year(),
		t.Hour(), t.Minute(), t.Second(), i)
}

func checkAndCreate(path string) (bool, error) {
	// detect if file exists
	var _, err = os.Stat(path)
	if err == nil {
		return true, err
	}
	// create dirs if file not exists
	if os.IsNotExist(err) {
		if filepath.Dir(path) != "." {
			return false, os.MkdirAll(filepath.Dir(path), 0777)
		}
	}
	return false, err
}

func writeKey(db *DB, key []byte, seek, size uint32, t uint8) (err error) {
	cmd := &Cmd{Type: t, Seek: seek, Size: size, Key: key}
	//get buf from pool
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	//encode
	encoder := gob.NewEncoder(buf)
	encoder.Encode(cmd)
	//fmt.Println(buf.Len(), string(buf.Bytes()))

	lenbuf := make([]byte, 4) //i hope its safe
	binary.BigEndian.PutUint32(lenbuf, uint32(buf.Len()))

	//write

	_, _, err = db.Fkey.Write(lenbuf)
	if err != nil {
		return err
	}
	_, _, err = db.Fkey.Write(buf.Bytes())
	if err != nil {
		return err
	}
	if t == 0 {
		db.Btree.ReplaceOrInsert(cmd)
	}

	return err
}

// Set store val and key
func Set(file string, key, val []byte) (err error) {
	db, err := Open(file)
	if err != nil {
		return err
	}
	db.Mux.Lock()
	defer db.Mux.Unlock()

	if val != nil {
		seek, writed, err := db.Fval.Write(val)
		if err != nil {
			return err
		}

		err = writeKey(db, key, uint32(seek), uint32(writed), 0)
	} else {
		err = writeKey(db, key, uint32(0), uint32(0), 0)
	}

	return err
}

// Close close file key and file val and delete db from map
func Close(file string) (err error) {
	db, ok := dbs[file]
	if !ok {
		return ErrDbNotOpen
	}
	err = db.Fkey.Close()
	err = db.Fkey.Close()
	delete(dbs, file)
	return err
}

// Open create file (with dirs) or read keys to map
// Save for multiple open
func Open(file string) (db *DB, err error) {
	var ok bool
	db, ok = dbs[file]
	if ok {
		return db, nil
	}
	exists, err := checkAndCreate(file)
	if exists && err != nil {
		return nil, err
	}
	//files
	fk, err := syncfile.NewSyncFile(file+".idx", FileMode)
	if err != nil {
		return nil, err
	}
	fv, err := syncfile.NewSyncFile(file, FileMode)
	if err != nil {
		return nil, err
	}
	if !exists {
		//new DB
		db = &DB{
			Btree: btree.New(16, nil),
			Mux:   new(sync.RWMutex),
			Fkey:  fk,
			Fval:  fv,
		}
		dbs[file] = db
	} else {
		//read DB
		tree, err := readTree(fk)
		if err != nil {
			return nil, err
		}
		db = &DB{
			Btree: tree,
			Mux:   new(sync.RWMutex),
			Fkey:  fk,
			Fval:  fv,
		}
		dbs[file] = db
	}
	return db, nil
}

// Delete remove key from tree and add record to log
func Delete(file string, key []byte) (deleted bool, err error) {
	db, err := Open(file)
	if err != nil {
		return deleted, err
	}
	db.Mux.Lock()
	defer db.Mux.Unlock()
	res := db.Btree.Delete(&Cmd{Key: key})
	if res != nil {
		deleted = true
	}
	err = writeKey(db, key, uint32(0), uint32(0), 1)
	return deleted, err
}

// Get return value by key or nil and error
func Get(file string, key []byte) (val []byte, err error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}

	db.Mux.RLock()
	defer db.Mux.RUnlock()
	item := db.Btree.Get(&Cmd{Key: key})
	if item == nil {
		return nil, ErrKeyNotFound
	}
	kv := item.(*Cmd)
	return db.Fval.Read(int64(kv.Size), int64(kv.Seek))
}

func readTree(f *syncfile.SyncFile) (*btree.BTree, error) {
	//log("readtree")
	var btree = btree.New(16, nil)

	//get buf from pool
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	b, err := f.ReadFile()
	if err != nil {
		return nil, err
	}

	buf.Write(b)

	for buf.Len() > 0 {

		sb := buf.Next(4)
		nextSize := int(binary.BigEndian.Uint32(sb))

		b := buf.Next(nextSize)
		decoder := gob.NewDecoder(bytes.NewReader(b))
		var cmd = &Cmd{}
		err = decoder.Decode(cmd)
		if err != nil {
			break
		}
		switch cmd.Type {
		case 0:
			btree.ReplaceOrInsert(cmd)
		case 1:
			btree.Delete(cmd)
		}
		//fmt.Printf("%s %+v\n", string(cmd.Key), cmd)
	}
	//fmt.Printf(" %+v\n", dbs[file].Btree.Len())
	return btree, err
}

func (i1 *Cmd) Less(item btree.Item, ctx interface{}) bool {
	i2 := item.(*Cmd)
	if bytes.Compare(i1.Key, i2.Key) < 0 {
		return true
	}
	return false
}

// Keys return keys in asc/desc order (false - descending,true - ascending)
// if limit == 0 return all keys
// offset - skip count records
// If from not nil - return keys after from (from not included)
// If last byte of from == "*" - use as prefix
func Keys(file string, from []byte, limit, offset int, asc bool) ([][]byte, error) {
	var keys = make([][]byte, 0, 0)
	db, err := Open(file)
	if err != nil {
		return nil, err
	}

	db.Mux.RLock()
	defer db.Mux.RUnlock()

	var counter int
	var byPrefix bool
	if from != nil {
		lastByte := from[len(from)-1:]
		if bytes.Equal([]byte("*"), lastByte) {
			byPrefix = true

		}
	}
	iterator := func(item btree.Item) bool {
		kvi := item.(*Cmd)
		//log(kvi)

		if from != nil {
			if !byPrefix {
				if bytes.Equal(kvi.Key, from) {
					//found
					from = nil
				}
				return true
			} else {
				if len(kvi.Key) >= len(from)-1 {
					//extract prefix and compare
					//log(from[:len(from)-1])
					if !bytes.Equal(kvi.Key[:len(from)-1], from[:len(from)-1]) {
						return true
					}
				} else {
					return true
				}
			}
		}
		if counter < offset {
			counter++
			limit++
			return true
		}
		keys = append(keys, kvi.Key)
		counter++
		if counter == limit {
			return false
		}
		return true
	}
	if asc {
		db.Btree.Ascend(iterator)
	} else {
		db.Btree.Descend(iterator)
	}
	//fmt.Println(keys)
	return keys, nil
}

// Close all opened Db
func CloseAll() (err error) {
	for k, _ := range dbs {
		err = Close(k)
	}
	return err
}
