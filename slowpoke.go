// Package slowpoke implements a low-level in-files and in-memory key/value store in pure Go.
// It persists to disk, and uses locking for multiple
// readers and a single writer.
package slowpoke

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/tidwall/btree"
)

var (
	database       *DataBase
	ErrKeyIsNil    = errors.New("Error: key is nil")
	ErrKeyNotFound = errors.New("Error: key not found")
	ErrEmptyDbName = errors.New("Error: empty db name")
)

type KV struct {
	Key  []byte
	Seek uint32
	Size uint32
}

type DB struct {
	Btree   *btree.BTree
	FileKey *os.File
	FileVal *os.File
	Mux     *sync.RWMutex
}

type DataBase struct {
	DataBases     map[string]*DB
	FileDataBases *os.File
	Mux           *sync.RWMutex
}

func (i1 *KV) Less(item btree.Item, ctx interface{}) bool {
	i2 := item.(*KV)
	if bytes.Compare(i1.Key, i2.Key) < 0 {
		return true
	}
	return false
}

// Set adds the given key to the tree.
// If tree not exists it will be created
// If an item in the tree already equals the given one, it is removed from the tree and inserted.
//
// nil cannot be added to the tree (will error).
func Set(file string, key []byte, val []byte) error {
	if key == nil {
		return ErrKeyIsNil
	}
	var db *DB
	var err error
	var seek int64
	if db, err = GetDb(file); err != nil {
		log.Fatal(err)
	}
	db.Mux.Lock()
	defer db.Mux.Unlock()
	//write value
	if val != nil {
		if seek, err = db.FileVal.Seek(0, 2); err == nil {
			w := bufio.NewWriter(db.FileVal)
			if _, err = w.Write(val); err == nil {
				err = w.Flush()

			}
		}
	}
	if err != nil {
		db.FileVal.Sync()
		return err
	}

	//write key

	if _, err = db.FileKey.Seek(0, 2); err == nil {
		wk := bufio.NewWriter(db.FileKey)
		err = wk.WriteByte('+')
		if err == nil {
			//ignore error? what may happen?
			//size val
			lenbuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenbuf, uint32(len(val)))
			_, err = wk.Write(lenbuf)
			//seek val
			seekbuf := make([]byte, 4)
			binary.BigEndian.PutUint32(seekbuf, uint32(seek))
			_, err = wk.Write(seekbuf)
			//key
			_, err = wk.Write(key)
			//end line (why just byte 13 not work?)
			_, err = wk.WriteString("\n")
			err = wk.Flush()
		}
	}

	if err != nil {
		db.FileKey.Sync()
		return err
	}
	db.Btree.ReplaceOrInsert(&KV{Key: key, Seek: uint32(seek), Size: uint32(len(val))})
	return err
}

func Get(file string, key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrKeyIsNil
	}
	var db *DB
	var err error
	if db, err = GetDb(file); err != nil {
		log.Fatal(err)
	}
	db.Mux.RLock()
	defer db.Mux.RUnlock()
	item := db.Btree.Get(&KV{Key: key})
	if item == nil {
		return nil, ErrKeyNotFound
	}
	kv := item.(*KV)
	//fmt.Printf("kv:%+v \n", kv)
	if _, err = db.FileVal.Seek(int64(kv.Seek), 0); err == nil {
		byteSlice := make([]byte, kv.Size)
		if _, err := db.FileVal.Read(byteSlice); err == nil {
			//fmt.Printf("kv:%+v b:%s \n", kv, string(byteSlice))
			return byteSlice, nil
		}
	}
	return nil, err
}

// Keys return all keys in descend order
// if limit == 0 return all keys
func Keys(name string, limit, offset int) [][]byte {
	var keys = make([][]byte, 0, 0)
	var db *DB
	var ok bool
	database.Mux.RLock()
	db, ok = database.DataBases[name]
	database.Mux.RUnlock()
	if !ok {
		return keys
	}
	var counter int
	//fmt.Println("Keys")
	db.Btree.Descend(func(item btree.Item) bool {
		kvi := item.(*KV)
		//fmt.Printf("%+v\n", kvi)
		if counter < offset {
			return true
		}
		keys = append(keys, kvi.Key)
		counter++
		if counter == limit {
			return false
		}
		return true
	})
	//fmt.Println(keys)
	return keys
}

func Range(name string, from, to []byte, desc bool) [][]byte {
	var keys = make([][]byte, 0)
	var db *DB
	var ok bool
	database.Mux.RLock()
	db, ok = database.DataBases[name]
	database.Mux.RUnlock()
	if !ok {
		return keys
	}
	_ = db
	return keys
}

func GetDb(name string) (db *DB, err error) {
	database.Mux.RLock()
	var ok bool
	db, ok = database.DataBases[name]
	database.Mux.RUnlock()
	if !ok {
		//create db
		db, err = createDb(name)
		//write to databases
		database.Mux.Lock()
		database.FileDataBases.WriteString(name + "\n")
		database.FileDataBases.Sync()
		database.Mux.Unlock()
	}
	return db, err
}

// Database create/open slowpoke with all Dbs
// and init it
func InitDatabase() {

	//create all fields
	var err error
	database = &DataBase{}
	database.Mux = new(sync.RWMutex)
	database.FileDataBases, err = os.OpenFile("slowpoke.db", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	database.DataBases = make(map[string]*DB)
	//read dbs
	scanner := bufio.NewScanner(database.FileDataBases)
	// Scan for next token.
	for scanner.Scan() {
		if fileDb := scanner.Text(); fileDb != "" {
			if db, err := createDb(fileDb); err != nil {
				log.Fatal(err)
			} else {
				_ = db
				//readDb(db)
			}
		}
	}
}

func createDb(fileDb string) (*DB, error) {
	if fileDb == "" {
		return nil, ErrEmptyDbName
	}
	var err error
	var db = &DB{}
	db.Mux = new(sync.RWMutex)
	db.Btree = btree.New(16, nil)
	db.Mux.Lock()
	db.FileKey, err = os.OpenFile(fileDb+".poke", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		db.Mux.Unlock()
		return nil, err
	}
	db.FileVal, err = os.OpenFile(fileDb+".slow", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		db.Mux.Unlock()
		return nil, err
	}
	db.Mux.Unlock()
	//write to arr
	database.Mux.Lock()
	database.DataBases[fileDb] = db
	database.Mux.Unlock()

	return db, nil
}

func readDb(db *DB) (err error) {
	db.Mux.RLock()
	defer db.Mux.RUnlock()
	db.FileKey.Seek(0, 0)
	//db.Btree.Descend(func(item btree.Item) bool {
	//kvi := item.(*KV)
	//fmt.Printf("!!!%+v\n", kvi)

	//return true
	//})
	scanner := bufio.NewScanner(db.FileKey)
	// Scan for next token.
	for scanner.Scan() {
		//b := scanner.Bytes()
		if scanner.Bytes() != nil && len(scanner.Bytes()) > 9 {
			if scanner.Bytes()[0] == '+' {
				b := make([]byte, len(scanner.Bytes()))
				copy(b, scanner.Bytes())
				fmt.Println("b:", b[9:])
				db.Btree.ReplaceOrInsert(&KV{
					Size: binary.BigEndian.Uint32(b[1:5]),
					Seek: binary.BigEndian.Uint32(b[5:9]),
					Key:  b[9:],
				})

			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("reading standard input:", err)
	}
	db.Btree.Descend(func(item btree.Item) bool {
		kvi := item.(*KV)
		_ = kvi
		fmt.Printf("%+v\n", string(kvi.Key))

		return true
	})
	//db.FileKey.Seek(0, 2)
	return err
}

// CloseDatabase close all filedescriptors and main database file slowpoke
func CloseDatabase() {
	if database == nil || database.FileDataBases == nil || database.Mux == nil || database.DataBases == nil {
		//panic("datebase not inited")
		return
	}
	database.Mux.Lock()
	defer database.Mux.Unlock()
	database.FileDataBases.Sync() // do a sync but ignore the error
	if err := database.FileDataBases.Close(); err != nil {
		panic(err)
	}
	for _, d := range database.DataBases {
		d.Mux.Lock()
		d.FileKey.Sync()
		if err := d.FileKey.Close(); err != nil {
			d.Mux.Unlock()
			panic(err)
		}
		d.FileVal.Sync()
		if err := d.FileVal.Close(); err != nil {
			d.Mux.Unlock()
			panic(err)
		}
		d.Mux.Unlock()
	}
}
