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
	fmt.Printf("%02d.%02d.%04d %02d:%02d:%02d\t%s\n",
		t.Day(), t.Month(), t.Year(),
		t.Hour(), t.Minute(), t.Second(), i)
}

func checkAndCreate(path string) (bool, error) {
	// detect if file exists
	var _, err = os.Stat(path)
	if err == nil {
		return true, err
	}
	// create file if not exists
	if os.IsNotExist(err) {
		if filepath.Dir(path) != "." {
			return false, os.MkdirAll(filepath.Dir(path), 0777)
		}
	}
	return false, err
}

func writeVal(file string, val []byte) (seek int64, n int, err error) {
	exists, err := checkAndCreate(file)
	if exists && err != nil {
		return 0, 0, err
	}
	f, err := syncfile.NewSyncFile(file, FileMode)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	//fmt.Println(string(buf.Bytes()))
	return f.Write(val)
}

func writeKey(file string, key []byte, seek, size uint32) (int64, int, error) {
	fk, err := syncfile.NewSyncFile(file+".idx", FileMode)
	if err != nil {
		return 0, 0, err
	}
	defer fk.Close()

	cmd := &Cmd{Type: 0, Seek: seek, Size: size, Key: key}
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
	fk.Write(lenbuf)
	return fk.Write(buf.Bytes())
}

func Set(file string, key, val []byte) error {
	var err error
	seek, writed, err := writeVal(file, val)
	if err != nil {
		return err
	}

	_, _, err = writeKey(file, key, uint32(seek), uint32(writed))
	if err != nil {
		return err
	}
	return err
}

func Get(file string, key []byte) (val []byte, err error) {
	var db *DB //= &DB{Btree: btree.New(16, nil), Mux: new(sync.RWMutex)}
	//var ok bool
	_, ok := dbs[file]
	if !ok {
		db, err = readTree(file)
		if err != nil {
			return nil, err
		}
		dbs[file] = db
	} else {
		db = dbs[file]
	}
	//db, _ := dbs[file]
	//log(db)
	//_ = db
	db.Mux.RLock()
	defer db.Mux.RUnlock()
	item := db.Btree.Get(&Cmd{Key: key})
	if item == nil {
		return nil, ErrKeyNotFound
	}
	kv := item.(*Cmd)

	f, err := syncfile.NewSyncFile(file, FileMode)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return f.Read(int64(kv.Size), int64(kv.Seek))
}

func readTree(fileval string) (*DB, error) {
	log("readtree")
	file := fileval + ".idx"
	var db = &DB{Btree: btree.New(16, nil), Mux: new(sync.RWMutex)}
	db.Mux.Lock()
	defer db.Mux.Unlock()
	//dbs[file] = db

	exists, err := checkAndCreate(file)
	if !exists || err != nil {
		return nil, err
	}

	//get buf from pool
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	f, err := syncfile.NewSyncFile(file, FileMode)
	if err != nil {
		return nil, err
	}
	defer f.Close()

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
		if cmd.Type == 0 {
			db.Btree.ReplaceOrInsert(cmd)
		}

		//fmt.Printf("%s %+v\n", string(cmd.Key), cmd)
	}
	//fmt.Printf(" %+v\n", dbs[file].Btree.Len())
	log(db)
	return db, err
}

func (i1 *Cmd) Less(item btree.Item, ctx interface{}) bool {
	i2 := item.(*Cmd)
	if bytes.Compare(i1.Key, i2.Key) < 0 {
		return true
	}
	return false
}
