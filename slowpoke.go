// Package slowpoke implements a low-level key/value store in pure Go.
// Keys stored in memory, Value stored on disk
package slowpoke

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	// FileMode - file will be created in this mode
	FileMode = 0666
)

var (
	stores = make(map[string]*Db)
	mutex  = &sync.RWMutex{}

	// ErrKeyNotFound - key not found
	ErrKeyNotFound = errors.New("Error: key not found")
	// ErrDbOpened - db is opened
	ErrDbOpened = errors.New("Error: db is opened")
	// ErrDbNotOpen - db not open
	ErrDbNotOpen = errors.New("Error: db not open")

	bufPool = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

// Cmd - struct with commands stored in keys
type Cmd struct {
	Seek    uint32
	Size    uint32
	KeySeek uint32
}

type readResponse struct {
	val []byte
	err error
}

type readRequest struct {
	readKey      string
	responseChan chan readResponse
}

type writeResponse struct {
	err error
}

type writeRequest struct {
	readKey      string
	writeVal     []byte
	responseChan chan writeResponse
}

type deleteRequest struct {
	deleteKey    string
	responseChan chan struct{}
}

type keysResponse struct {
	keys [][]byte
}

type keysRequest struct {
	fromKey      []byte
	limit        uint32
	offset       uint32
	asc          bool
	responseChan chan keysResponse
}

type setsResponse struct {
	err error
}

type setsRequest struct {
	pairs        [][]byte
	responseChan chan setsResponse
}

type getsResponse struct {
	pairs [][]byte
}

type getsRequest struct {
	keys         [][]byte
	responseChan chan getsResponse
}

// Db store channels with requests
type Db struct {
	readRequests   chan readRequest
	writeRequests  chan writeRequest
	deleteRequests chan deleteRequest
	keysRequests   chan keysRequest
	setsRequests   chan setsRequest
	getsRequests   chan getsRequest
}

// newDb Create new Db
// it return error if any
// Db has finalizer for canceling goroutine
// File will be created (with dirs) if not exist
func newDb(file string) (*Db, error) {
	ctx, cancel := context.WithCancel(context.Background())
	readRequests := make(chan readRequest)
	writeRequests := make(chan writeRequest)
	deleteRequests := make(chan deleteRequest)
	keysRequests := make(chan keysRequest)
	setsRequests := make(chan setsRequest)
	getsRequests := make(chan getsRequest)
	d := &Db{
		readRequests:   readRequests,
		writeRequests:  writeRequests,
		deleteRequests: deleteRequests,
		keysRequests:   keysRequests,
		setsRequests:   setsRequests,
		getsRequests:   getsRequests,
	}
	// This is a lambda, so we don't have to add members to the struct
	runtime.SetFinalizer(d, func(dict *Db) {
		cancel()
	})

	exists, err := checkAndCreate(file)
	if exists && err != nil {
		cancel()
		return nil, err
	}
	//files
	fk, err := os.OpenFile(file+".idx", os.O_CREATE|os.O_RDWR, FileMode) //syncfile.NewSyncFile(file+".idx", FileMode)
	if err != nil {
		cancel()
		return nil, err
	}
	fv, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, FileMode)
	if err != nil {
		cancel()
		return nil, err
	}

	// We can't have run be a method of Db, because otherwise then the goroutine will keep the reference alive
	go run(ctx, fk, fv, readRequests, writeRequests, deleteRequests, keysRequests, setsRequests, getsRequests)

	return d, nil
}

// checkAndCreate may create dirs
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

// run read keys from *.idx store and run listeners
func run(parentCtx context.Context, fk *os.File, fv *os.File,
	readRequests <-chan readRequest, writeRequests <-chan writeRequest,
	deleteRequests <-chan deleteRequest, keysRequests <-chan keysRequest,
	setsRequests <-chan setsRequest, getsRequests <-chan getsRequest) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// valDict map with key and address of values
	valDict := make(map[string]*Cmd)
	// keysDict store ordered slice of keys
	var keysDict [][]byte
	keysDict = make([][]byte, 0)

	//delete key from slice keysDict
	deleteFromKeys := func(b []byte) {
		//fmt.Printf("before sort keys:%+v\n", keysDict)
		sort.Slice(keysDict, func(i, j int) bool {
			return bytes.Compare(keysDict[i], keysDict[j]) <= 0
		})
		//fmt.Printf("after sort keys:%+v\n", keysDict)
		found := sort.Search(len(keysDict), func(i int) bool {
			return bytes.Compare(keysDict[i], b) >= 0
		})
		if found >= 0 && found < len(keysDict) {
			//fmt.Printf("found:%d key:%+v keys:%+v\n", found, b, keysDict)
			//is found return 0 if not found?
			if bytes.Equal(keysDict[found], b) {
				keysDict = append(keysDict[:found], keysDict[found+1:]...)
			}
		}
	}
	//read keys
	//get buf from pool
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	b, _ := ioutil.ReadAll(fk) //fk.ReadFile()
	buf.Write(b)
	var readSeek uint32
	for buf.Len() > 0 {
		_ = uint8(buf.Next(1)[0]) //format version
		t := uint8(buf.Next(1)[0])
		seek := binary.BigEndian.Uint32(buf.Next(4))
		size := binary.BigEndian.Uint32(buf.Next(4))
		_ = buf.Next(4) //time
		sizeKey := int(binary.BigEndian.Uint16(buf.Next(2)))
		key := buf.Next(sizeKey)
		strkey := string(key)
		cmd := &Cmd{
			Seek:    seek,
			Size:    size,
			KeySeek: readSeek,
		}
		readSeek += uint32(16 + sizeKey)
		switch t {
		case 0:
			if _, exists := valDict[strkey]; !exists {
				//write new key at keys store
				keysDict = append(keysDict, key)
			}
			valDict[strkey] = cmd
		case 1:
			delete(valDict, strkey)
			deleteFromKeys(key)
		}
	}
	/*
		for k, v := range valDict {
			fmt.Printf("%+v:%+v\n", k, v)
		}
	*/

	for {
		select {
		case <-ctx.Done():
			// start on Close()
			fk.Close()
			fv.Close()
			//fmt.Println("done")
			return nil
		case dr := <-deleteRequests:
			//fmt.Println("del")
			//for _, v := range valDict {
			//fmt.Printf("%+v\n", v)
			//}
			delete(valDict, dr.deleteKey)
			deleteFromKeys([]byte(dr.deleteKey))
			// delete command append to the end of keys file
			writeKey(fk, 1, 0, 0, []byte(dr.deleteKey), true, -1)
			close(dr.responseChan)
		case wr := <-writeRequests:
			var err error
			var seek, newSeek int64
			cmd := &Cmd{Size: uint32(len(wr.writeVal))}
			if val, exists := valDict[wr.readKey]; exists {
				// key exists
				cmd.Seek = val.Seek
				cmd.KeySeek = val.KeySeek
				if val.Size >= uint32(len(wr.writeVal)) {
					//write at old seek new value
					_, _, err = writeAtPos(fv, wr.writeVal, int64(val.Seek), true) //fv.WriteAt(wr.writeVal, int64(val.Seek))
				} else {
					//write at new seek (at the end of file)
					seek, _, err = writeAtPos(fv, wr.writeVal, int64(-1), true) //fv.Write(wr.writeVal)
					cmd.Seek = uint32(seek)
				}
				if err == nil {
					// if no error - store key at KeySeek
					newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, []byte(wr.readKey), true, int64(cmd.KeySeek))
					cmd.KeySeek = uint32(newSeek)
				}
			} else {
				// new key
				// write value at the end of file
				seek, _, err = writeAtPos(fv, wr.writeVal, int64(-1), true) //fv.Write(wr.writeVal)
				cmd.Seek = uint32(seek)
				//fmt.Println(fv, wr.readKey, string(wr.writeVal), cmd.Seek, err)

				//write new key at keys store
				keysDict = append(keysDict, []byte(wr.readKey))

				if err == nil {
					newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, []byte(wr.readKey), true, -1)
					cmd.KeySeek = uint32(newSeek)
				}
			}

			if err == nil {
				//fmt.Printf("wr:%s %+v\n", wr.readKey, cmd)
				// store value address
				valDict[wr.readKey] = cmd
			}

			wr.responseChan <- writeResponse{err}
		case rr := <-readRequests:
			if val, exists := valDict[rr.readKey]; exists {
				//fmt.Printf("rr:%s %+v\n", rr.readKey, val)
				b := make([]byte, val.Size)
				_, err := fv.ReadAt(b, int64(val.Seek))
				rr.responseChan <- readResponse{b, err}
			} else {
				// if no key return eror
				rr.responseChan <- readResponse{nil, ErrKeyNotFound}
			}

		case kr := <-keysRequests:

			//sort slice
			//TODO may be store sort state in bool
			sorted := sort.SliceIsSorted(keysDict, func(i, j int) bool {
				return bytes.Compare(keysDict[i], keysDict[j]) <= 0
			})
			if !sorted {
				sort.Slice(keysDict, func(i, j int) bool {
					return bytes.Compare(keysDict[i], keysDict[j]) <= 0
				})
			}
			var result [][]byte
			result = make([][]byte, 0)
			lenKeys := len(keysDict)
			var start, end, found int
			var byPrefix bool
			found = -1
			if kr.fromKey != nil {
				if bytes.Equal(kr.fromKey[len(kr.fromKey)-1:], []byte("*")) {
					byPrefix = true
					_ = byPrefix
					kr.fromKey = kr.fromKey[:len(kr.fromKey)-1]
					//fmt.Println(string(kr.fromKey))
				}
				found = sort.Search(lenKeys, func(i int) bool {
					//bynary search may return not eq result
					return bytes.Compare(keysDict[i], kr.fromKey) >= 0
				})
				if !kr.asc && byPrefix {
					//iterate if desc and by prefix
					for j := lenKeys - 1; j >= 0; j-- {
						if len(keysDict[j]) >= len(kr.fromKey) {
							if bytes.Compare(keysDict[j][:len(kr.fromKey)], kr.fromKey) == 0 {
								found = j
								break
								//fmt.Println("found:", found, string(keysDict[j][:len(kr.fromKey)]), string(keysDict[j]))
							}
						}
					}
				}
				if found == lenKeys {
					//not found
					found = -1
				} else {
					//found
					if !byPrefix && !bytes.Equal(keysDict[found], kr.fromKey) {
						found = -1 //not eq
					}
				}
				// if not found - found will == len and return empty array
				//fmt.Println(string(kr.fromKey), found)
			}
			// ascending order
			if kr.asc {
				start = 0
				if kr.fromKey != nil {
					if found == -1 {
						start = lenKeys
					} else {
						start = found + 1
						if byPrefix {
							//include
							start = found
						}
					}
				}
				if kr.offset > 0 {
					start += int(kr.offset)
				}
				end = lenKeys
				if kr.limit > 0 {
					end = start + int(kr.limit)
					if end > lenKeys {
						end = lenKeys
					}
				}
				if start < lenKeys {
					for i := start; i < end; i++ {
						if byPrefix {
							if len(keysDict[i]) < len(kr.fromKey) {
								break
							} else {
								//compare with prefix
								//fmt.Println("prefix", string(keysDict[i][:len(kr.fromKey)]), string(kr.fromKey))
								if !bytes.Equal(keysDict[i][:len(kr.fromKey)], kr.fromKey) {
									break
								}
							}
						}
						result = append(result, keysDict[i])
					}
				}
			} else {
				//descending
				start = lenKeys - 1
				if kr.fromKey != nil {
					if found == -1 {
						start = -1
					} else {
						start = found - 1
						if byPrefix {
							//include
							start = found
						}
					}
				}

				if kr.offset > 0 {
					start -= int(kr.offset)
				}
				end = 0
				if kr.limit > 0 {
					end = start - int(kr.limit) + 1
					if end < 0 {
						end = 0
					}
				}
				if start >= 0 {
					for i := start; i >= end; i-- {
						if byPrefix {
							if len(keysDict[i]) < len(kr.fromKey) {
								break
							} else {
								//compare with prefix
								//fmt.Println("prefix", string(keysDict[i][:len(kr.fromKey)]), string(kr.fromKey))
								if !bytes.Equal(keysDict[i][:len(kr.fromKey)], kr.fromKey) {
									break
								}
							}
						}
						result = append(result, keysDict[i])
					}
				}
			}
			kr.responseChan <- keysResponse{keys: result}
			close(kr.responseChan)
		case sr := <-setsRequests:
			var err error
			var seek, newSeek int64
			for i := range sr.pairs {
				if i%2 != 0 {
					// on odd - append val and store key
					if sr.pairs[i] == nil || sr.pairs[i-1] == nil {
						break
					}
					//key - sr.pairs[i-1]
					//val - sr.pairs[i]
					cmd := &Cmd{Size: uint32(len(sr.pairs[i]))}
					seek, _, err = writeAtPos(fv, sr.pairs[i], int64(-1), false) //fv.WriteNoSync(sr.pairs[i])
					cmd.Seek = uint32(seek)
					if err != nil {
						break
					}

					newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, sr.pairs[i-1], false, -1)
					cmd.KeySeek = uint32(newSeek)
					if err != nil {
						break
					}
					keyStr := string(sr.pairs[i-1])
					if _, exists := valDict[keyStr]; !exists {
						//write new key at keys store
						keysDict = append(keysDict, sr.pairs[i-1])
					}
					valDict[keyStr] = cmd

				}
			}
			if err == nil {
				err = fk.Sync()
				if err == nil {
					err = fv.Sync()
				}
			}

			sr.responseChan <- setsResponse{err}
		case gr := <-getsRequests:
			var result [][]byte
			result = make([][]byte, 0)
			for _, key := range gr.keys {
				if val, exists := valDict[string(key)]; exists {
					//val, _ := fv.Read(int64(val.Size), int64(val.Seek))
					b := make([]byte, val.Size)
					fv.ReadAt(b, int64(val.Seek))
					result = append(result, key)
					result = append(result, b)
				}
			}
			gr.responseChan <- getsResponse{result}
		}
	}
}

// writeAtPos store bytes to file
// if pos<0 store at the end of file
// if withSync == true - do sync on write
func writeAtPos(f *os.File, b []byte, pos int64, withSync bool) (seek int64, n int, err error) {
	seek = pos
	if pos < 0 {
		seek, err = f.Seek(0, 2)
		if err != nil {
			return seek, 0, err
		}
	}
	n, err = f.WriteAt(b, seek)
	if err != nil {
		return seek, n, err
	}
	if withSync {
		return seek, n, f.Sync() // ensure that the write is done.
	}
	return seek, n, err
}

// writeKey create buffer and store key with val address and size
func writeKey(fk *os.File, t uint8, seek, size uint32, key []byte, sync bool, keySeek int64) (newSeek int64, err error) {
	//get buf from pool
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	buf.Grow(16 + len(key))

	//encode
	binary.Write(buf, binary.BigEndian, uint8(0))                  //1byte
	binary.Write(buf, binary.BigEndian, t)                         //1byte
	binary.Write(buf, binary.BigEndian, seek)                      //4byte
	binary.Write(buf, binary.BigEndian, size)                      //4byte
	binary.Write(buf, binary.BigEndian, uint32(time.Now().Unix())) //4byte
	binary.Write(buf, binary.BigEndian, uint16(len(key)))          //2byte
	buf.Write(key)

	if sync {
		if keySeek < 0 {
			newSeek, _, err = writeAtPos(fk, buf.Bytes(), int64(-1), true) //fk.Write(buf.Bytes())
		} else {
			newSeek, _, err = writeAtPos(fk, buf.Bytes(), int64(keySeek), true) //fk.WriteAt(buf.Bytes(), int64(keySeek))
		}

	} else {
		newSeek, _, err = writeAtPos(fk, buf.Bytes(), int64(-1), false) //fk.WriteNoSync(buf.Bytes())
	}

	return newSeek, err
}

// internal set
func (dict *Db) setKey(key string, val []byte) error {
	c := make(chan writeResponse)
	w := writeRequest{readKey: key, writeVal: val, responseChan: c}
	dict.writeRequests <- w
	resp := <-c
	return resp.err
}

// internal get
func (dict *Db) readKey(key string) ([]byte, error) {
	c := make(chan readResponse)
	w := readRequest{readKey: key, responseChan: c}
	dict.readRequests <- w
	resp := <-c
	return resp.val, resp.err
}

// internal delete
func (dict *Db) deleteKey(key string) {
	c := make(chan struct{})
	d := deleteRequest{deleteKey: key, responseChan: c}
	dict.deleteRequests <- d
	<-c
}

// internal keys
func (dict *Db) readKeys(from []byte, limit, offset uint32, asc bool) [][]byte {
	c := make(chan keysResponse)
	w := keysRequest{responseChan: c, fromKey: from, limit: limit, offset: offset, asc: asc}
	dict.keysRequests <- w
	resp := <-c
	return resp.keys
}

// internal sets
func (dict *Db) sets(setPairs [][]byte) error {
	c := make(chan setsResponse)
	w := setsRequest{pairs: setPairs, responseChan: c}
	dict.setsRequests <- w
	resp := <-c
	return resp.err
}

// internal gets
func (dict *Db) gets(keys [][]byte) [][]byte {
	c := make(chan getsResponse)
	w := getsRequest{keys: keys, responseChan: c}
	dict.getsRequests <- w
	resp := <-c
	return resp.pairs
}

// Set store val and key with sync at end
// File - may be existing file or new
// If path to file contains dirs - dirs will be created
// If val is nil - will store only key
func Set(file string, key []byte, val []byte) (err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	err = db.setKey(string(key), val)
	return err
}

// Open open/create Db (with dirs)
// This operation is locked by mutex
// Return error if any
// Create .idx file for key storage
func Open(file string) (db *Db, err error) {
	mutex.Lock()
	defer mutex.Unlock()

	v, ok := stores[file]
	if ok {
		return v, nil
	}

	//fmt.Println("NewDb")
	db, err = newDb(file)
	if err == nil {
		stores[file] = db
	}
	return db, err
}

// Get return value by key or nil and error
// Get will open Db if it closed
// return error if any
func Get(file string, key []byte) (val []byte, err error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}
	val, err = db.readKey(string(key))
	return val, err
}

// Keys return keys in ascending  or descending order (false - descending,true - ascending)
// if limit == 0 return all keys
// if offset>0 - skip offset records
// If from not nil - return keys after from (from not included)
// If last byte of from == "*" - return keys with this prefix
func Keys(file string, from []byte, limit, offset uint32, asc bool) ([][]byte, error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}
	val := db.readKeys(from, limit, offset, asc)
	return val, err
}

// Close - close Db and free used memory
// It run finalizer and cancel goroutine
func Close(file string) (err error) {
	mutex.Lock()
	defer mutex.Unlock()
	_, ok := stores[file]
	if !ok {
		return ErrDbNotOpen
	}
	delete(stores, file)
	/* Force GC, to require finalizer to run */
	runtime.GC()
	return err
}

// CloseAll - close all opened Db
func CloseAll() (err error) {

	for k := range stores {
		err = Close(k)
		if err != nil {
			break
		}
	}

	return err
}

// DeleteFile close file key and file val and delete db from map and disk
// All data will be loss!
func DeleteFile(file string) (err error) {
	Close(file)

	err = os.Remove(file)
	if err != nil {
		return err
	}
	err = os.Remove(file + ".idx")
	return err
}

// Gets return key/value pairs in random order
// result contains key and value
// Gets not return error if key not found
// If no keys found return empty result
func Gets(file string, keys [][]byte) (result [][]byte) {
	db, err := Open(file)
	if err != nil {
		return nil
	}
	return db.gets(keys)
}

// Sets store vals and keys
// Sync will called only at end of insertion
// Use it for mass insertion
// every pair must contain key and value
func Sets(file string, pairs [][]byte) (err error) {

	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	err = db.sets(pairs)
	return err
}

// Delete key (always return true)
// Delete not remove any data from files
// Return error if any
func Delete(file string, key []byte) (deleted bool, err error) {
	db, err := Open(file)
	if err != nil {
		return deleted, err
	}
	db.deleteKey(string(key))
	return true, err
}
