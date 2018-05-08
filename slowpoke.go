// Package slowpoke implements a low-level key/value store in pure Go.
// Keys stored in memory, Value stored on disk
package slowpoke

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
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

type hasResponse struct {
	exists bool
}

type hasRequest struct {
	key          string
	responseChan chan hasResponse
}

type countResponse struct {
	count int
}

type countRequest struct {
	responseChan chan countResponse
}

type counterResponse struct {
	counter uint64
}

type counterRequest struct {
	key          string
	responseChan chan counterResponse
}

type counterSetRequest struct {
	key          string
	counter      uint64
	store        bool
	responseChan chan struct{}
}

// Db store channels with requests
type Db struct {
	readRequests       chan readRequest
	writeRequests      chan writeRequest
	deleteRequests     chan deleteRequest
	keysRequests       chan keysRequest
	setsRequests       chan setsRequest
	getsRequests       chan getsRequest
	hasRequests        chan hasRequest
	countRequests      chan countRequest
	counterRequests    chan counterRequest
	counterSetRequests chan counterSetRequest
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
	hasRequests := make(chan hasRequest)
	countRequests := make(chan countRequest)
	counterRequests := make(chan counterRequest)
	counterSetRequests := make(chan counterSetRequest)
	d := &Db{
		readRequests:       readRequests,
		writeRequests:      writeRequests,
		deleteRequests:     deleteRequests,
		keysRequests:       keysRequests,
		setsRequests:       setsRequests,
		getsRequests:       getsRequests,
		hasRequests:        hasRequests,
		countRequests:      countRequests,
		counterRequests:    counterRequests,
		counterSetRequests: counterSetRequests,
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
	go run(ctx, fk, fv, readRequests, writeRequests, deleteRequests, keysRequests, setsRequests, getsRequests,
		hasRequests, countRequests, counterRequests, counterSetRequests)

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
	setsRequests <-chan setsRequest, getsRequests <-chan getsRequest,
	hasRequests <-chan hasRequest, countRequests <-chan countRequest,
	counterRequests <-chan counterRequest, counterSetRequests <-chan counterSetRequest) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// valDict map with key and address of values
	valDict := make(map[string]*Cmd)
	// keysDict store ordered slice of keys
	var keysDict = make([][]byte, 0)
	// countersDict store counters
	countersDict := make(map[string]uint64)

	//delete key from slice keysDict
	deleteFromKeys := func(b []byte) {
		found := sort.Search(len(keysDict), func(i int) bool {
			return bytes.Compare(keysDict[i], b) >= 0
		})
		if found < len(keysDict) {
			//fmt.Printf("found:%d key:%+v keys:%+v\n", found, b, keysDict)
			if bytes.Equal(keysDict[found], b) {
				keysDict = append(keysDict[:found], keysDict[found+1:]...)
			}
		}
	}

	//appendAsc insert key in slice in ascending order
	appendAsc := func(b []byte) {
		keysLen := len(keysDict)
		found := sort.Search(keysLen, func(i int) bool {
			return bytes.Compare(keysDict[i], b) >= 0
		})
		if found == 0 {
			//prepend
			keysDict = append([][]byte{b}, keysDict...)

		} else {
			if found >= keysLen {
				//not found - postpend ;)
				keysDict = append(keysDict, b)
			} else {
				//found
				//https://blog.golang.org/go-slices-usage-and-internals
				keysDict = append(keysDict, nil)           //grow origin slice capacity if needed
				copy(keysDict[found+1:], keysDict[found:]) //ha-ha, lol, 20x faster
				keysDict[found] = b
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
				//keysDict = append(keysDict, key)
				appendAsc(key)
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
			oldCmd, exists := valDict[wr.readKey]
			cmd, err := writeKeyVal(fk, fv, wr.readKey, wr.writeVal, exists, oldCmd)
			if !exists {
				appendAsc([]byte(wr.readKey))
			}
			if err == nil {
				//fmt.Printf("wr:%s %+v\n", wr.readKey, cmd)
				// store command if no error
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
					found = lenKeys
					for j := lenKeys - 1; j >= 0; j-- {
						if len(keysDict[j]) >= len(kr.fromKey) {
							if bytes.Equal(keysDict[j][:len(kr.fromKey)], kr.fromKey) {
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
						appendAsc(sr.pairs[i-1])
						//keysDict = append(keysDict, sr.pairs[i-1])
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
		case hr := <-hasRequests:
			_, exists := valDict[hr.key]
			hr.responseChan <- hasResponse{exists: exists}
		case cr := <-countRequests:
			cr.responseChan <- countResponse{len(keysDict)}
		case cntrr := <-counterRequests:
			val, _ := countersDict[cntrr.key]
			val++
			countersDict[cntrr.key] = val
			cntrr.responseChan <- counterResponse{counter: val}
		case cntrSetr := <-counterSetRequests:
			if cntrSetr.store {
				for k, v := range countersDict {
					//store current counter
					//fmt.Printf("%+v:%+v\n", k, v)
					oldCmd, exists := valDict[k]
					if v > 0 {

						buf := make([]byte, 8)
						binary.BigEndian.PutUint64(buf, v)

						writeKeyVal(fk, fv, k, buf, exists, oldCmd)
					}
				}
			} else {
				countersDict[cntrSetr.key] = cntrSetr.counter
			}

			close(cntrSetr.responseChan)
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
	binary.Write(buf, binary.BigEndian, uint8(0))                  //1byte version
	binary.Write(buf, binary.BigEndian, t)                         //1byte command code(0-set,1-delete)
	binary.Write(buf, binary.BigEndian, seek)                      //4byte seek
	binary.Write(buf, binary.BigEndian, size)                      //4byte size
	binary.Write(buf, binary.BigEndian, uint32(time.Now().Unix())) //4byte timestamp
	binary.Write(buf, binary.BigEndian, uint16(len(key)))          //2byte key size
	buf.Write(key)                                                 //key

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

func writeKeyVal(fk *os.File, fv *os.File, readKey string, writeVal []byte, exists bool, oldCmd *Cmd) (cmd *Cmd, err error) {

	var seek, newSeek int64
	cmd = &Cmd{Size: uint32(len(writeVal))}
	if exists {
		// key exists
		cmd.Seek = oldCmd.Seek
		cmd.KeySeek = oldCmd.KeySeek
		if oldCmd.Size >= uint32(len(writeVal)) {
			//write at old seek new value
			_, _, err = writeAtPos(fv, writeVal, int64(oldCmd.Seek), true)
		} else {
			//write at new seek (at the end of file)
			seek, _, err = writeAtPos(fv, writeVal, int64(-1), true)
			cmd.Seek = uint32(seek)
		}
		if err == nil {
			// if no error - store key at KeySeek
			newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, []byte(readKey), true, int64(cmd.KeySeek))
			cmd.KeySeek = uint32(newSeek)
		}
	} else {
		// new key
		// write value at the end of file
		seek, _, err = writeAtPos(fv, writeVal, int64(-1), true)
		cmd.Seek = uint32(seek)
		if err == nil {
			newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, []byte(readKey), true, -1)
			cmd.KeySeek = uint32(newSeek)
		}
	}
	return cmd, err
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

// internal has
func (dict *Db) has(key string) bool {
	c := make(chan hasResponse)
	w := hasRequest{key: key, responseChan: c}
	dict.hasRequests <- w
	resp := <-c
	return resp.exists
}

// internal count
func (dict *Db) countKeys() int {
	c := make(chan countResponse)
	w := countRequest{responseChan: c}
	dict.countRequests <- w
	resp := <-c
	return resp.count
}

// internal counter
func (dict *Db) counterGet(key string) uint64 {
	c := make(chan counterResponse)
	w := counterRequest{key: key, responseChan: c}
	dict.counterRequests <- w
	resp := <-c
	return resp.counter
}

// internal counter
func (dict *Db) counterSet(key string, counterNewVal uint64, store bool) {
	c := make(chan struct{})
	w := counterSetRequest{key: key, counter: counterNewVal, store: store, responseChan: c}
	dict.counterSetRequests <- w
	<-c
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

// Put store val and key with sync at end. It's wrapper for Set.
func Put(file string, key []byte, val []byte) (err error) {
	return Set(file, key, val)
}

// SetGob - experimental future for lazy usage, see tests
func SetGob(file string, key interface{}, val interface{}) (err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	bufKey := bytes.Buffer{}
	bufVal := bytes.Buffer{}

	err = gob.NewEncoder(&bufKey).Encode(key)
	if err != nil {
		return err
	}
	err = gob.NewEncoder(&bufVal).Encode(val)
	if err != nil {
		return err
	}

	err = db.setKey(bufKey.String(), bufVal.Bytes())
	return err
}

// Has return true if key exist or error if any
func Has(file string, key []byte) (exist bool, err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return false, err
	}
	exist = db.has(string(key))
	return exist, err
}

// Count return count of keys or error if any
func Count(file string) (cnt int, err error) {
	db, err := Open(file)
	if err != nil {
		return 0, err
	}
	cnt = db.countKeys()
	return cnt, err
}

// Counter return unique uint64
func Counter(file string, key []byte) (counter uint64, err error) {
	db, err := Open(file)
	if err != nil {
		return 0, err
	}
	counter = db.counterGet(string(key))
	//fmt.Println("Counter", counter)
	if counter == 1 {
		// new counter
		b, _ := db.readKey(string(key))
		if b != nil && len(b) == 8 {
			//recover in case of panic?
			counter = binary.BigEndian.Uint64(b)
			counter++
		}

		db.counterSet(string(key), counter, false)
	}
	return counter, err
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

// GetGob - experimental future for lazy usage, see tests
func GetGob(file string, key interface{}, val interface{}) (err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	err = gob.NewEncoder(buf).Encode(key)
	if err != nil {
		return err
	}

	bin, err := db.readKey(buf.String())
	buf.Reset()
	if err != nil {
		return err
	}
	buf.Write(bin)
	err = gob.NewDecoder(buf).Decode(val)

	return err

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
	db, ok := stores[file]
	if !ok {
		return ErrDbNotOpen
	} else {
		db.counterSet("", 0, true)
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
