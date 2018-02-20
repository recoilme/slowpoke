package main

import (
	"bufio"
	"bytes"
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
	ErrEmptyDbName = errors.New("Error: empty db name")
)

type KV struct {
	Key   []byte
	Value []byte
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

func main() {
	InitDatabase()
	defer CloseDatabase()
	Set("users", []byte("aa"))
	Set("users", []byte("a"))
	Set("msg", []byte("msg"))
	keys := Keys("msg")
	for _, k := range keys {
		fmt.Println(":", string(k), ":")
	}

	key, err := Get("users", []byte("aa"))
	fmt.Println("key:", string(key), "err:", err)
}

// Set adds the given key to the tree.
// If tree not exists it will be created
// If an item in the tree already equals the given one, it is removed from the tree and inserted.
//
// nil cannot be added to the tree (will error).
func Set(file string, key []byte) error {
	if key == nil {
		return ErrKeyIsNil
	}
	var db *DB
	var err error
	if db, err = GetDb(file); err != nil {
		log.Fatal(err)
	}
	db.Mux.Lock()
	defer db.Mux.Unlock()
	if _, err = db.FileKey.Seek(0, 2); err == nil {
		w := bufio.NewWriter(db.FileKey)
		w.WriteByte('+')
		if _, err = w.Write(key); err == nil {
			w.WriteString("\n")
			if err = w.Flush(); err == nil {
				db.Btree.ReplaceOrInsert(&KV{Key: key})

			}
		}
	}
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
	kvi := item.(*KV)
	return kvi.Key, nil
}

// Keys return all keys in descend order
func Keys(name string) [][]byte {
	var keys = make([][]byte, 0)
	var db *DB
	var ok bool
	database.Mux.RLock()
	db, ok = database.DataBases[name]
	database.Mux.RUnlock()
	if !ok {
		return keys
	}
	db.Btree.Descend(func(item btree.Item) bool {
		kvi := item.(*KV)
		keys = append(keys, kvi.Key)
		return true
	})
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
		database.Mux.Unlock()
	}
	return db, err
}

// Database create/open slowpoke with all Dbs
// and init it
func InitDatabase() {
	CloseDatabase()
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
				readDb(db)
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
	scanner := bufio.NewScanner(db.FileKey)
	// Scan for next token.
	for scanner.Scan() {
		//fmt.Println(scanner.Text())

		//b := scanner.Bytes()
		if scanner.Bytes() != nil && len(scanner.Bytes()) > 0 {
			if scanner.Bytes()[0] == '+' {
				//fmt.Println(db.FileKey.Name(), "!", string(scanner.Bytes()[1:]), "!")
				db.Btree.ReplaceOrInsert(&KV{Key: scanner.Bytes()[1:]})
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("reading standard input:", err)
	}
	db.FileKey.Seek(0, 2)
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

/*
	muxDbs = new(sync.RWMutex)
	muxDb = new(sync.RWMutex)
	database = &DataBase{}
	database.DataBases = make(map[string]*DB)
	var err error
	database.FileDataBases, err = os.OpenFile("slowpoke.db", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	if e := Set("users", []byte("a")); e != nil {
		panic(e)
	}
	Set("users", []byte("aa"))
	Set("users", []byte("b"))
	Set("users", []byte("bb"))
	db := getDb("users")
	db.T.Descend(iterator)

	for i := 8; i <= 12; i++ {
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, uint32(i))
		Set("userids", bs)
	}
	userids := getDb("userids")

	userids.T.Descend(func(item btree.Item) bool {
		kvi := item.(*KV)
		k := binary.BigEndian.Uint32(kvi.Key)
		fmt.Printf("Descend: %d \n", k)
		return true
	})

	userids.T.Ascend(func(item btree.Item) bool {
		kvi := item.(*KV)
		k := binary.BigEndian.Uint32(kvi.Key)

		fmt.Printf("Ascend: %d \n", k)
		return true
	})
*/

/*
// Set adds the given key to the tree.
// If tree not exists it will be created
// If an item in the tree already equals the given one, it is removed from the tree and inserted.
//
// nil cannot be added to the tree (will error).
func Set(file string, key []byte) error {
	if key == nil {
		return errors.New("key is nil")
	}
	db := getDb(file)
	if db.T == nil {
		return errors.New("tree is nil")
	}
	m.Lock()
	db.T.ReplaceOrInsert(&KV{Key: key})
	m.Unlock()
	return nil
}

func getDb(file string) (db *DB) {
	var ok bool
	mmap.RLock()
	db, ok = dbs[file]
	mmap.RUnlock()
	if !ok {
		fmt.Println("new db")
		mmap.Lock()
		db = &DB{T: btree.New(16, nil)}
		dbs[file] = db
		mmap.Unlock()
	}

	return db
}

func iterator(item btree.Item) bool {
	kvi := item.(*KV)
	fmt.Printf("iterator: %s \n", string(kvi.Key))
	return true
}

*/
/*output:
iterator: bb
iterator: b
iterator: aa
iterator: a
Descend: 12
Descend: 11
Descend: 10
Descend: 9
Descend: 8
Ascend: 8
Ascend: 9
Ascend: 10
Ascend: 11
Ascend: 12
*/
