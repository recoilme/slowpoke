package chandict

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/recoilme/syncfile"
)

const (
	// FileMode - file will be created in this mode
	FileMode = 0666
)

var (
	stores = make(map[string]*ChanDict)
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

// Cmd - struct with commands
type Cmd struct {
	Seek    uint32
	Size    uint32
	KeySeek uint32
}

type readRequestResponse struct {
	val []byte
	err error
}

type readRequest struct {
	readKey      string
	responseChan chan readRequestResponse
}

type writeRequestResponse struct {
	err error
}

type writeRequest struct {
	readKey      string
	writeVal     []byte
	responseChan chan writeRequestResponse
}

type casRequest struct {
	setOnNotExists bool
	readKey        string
	oldVal         []byte
	newVal         []byte
	responseChan   chan bool
}

type deleteRequest struct {
	deleteKey    string
	responseChan chan struct{}
}

type keyRequestResponse struct {
	keys [][]byte
}

type keyRequest struct {
	fromKey      []byte
	limit        uint32
	offset       uint32
	asc          bool
	responseChan chan keyRequestResponse
}

type ChanDict struct {
	readRequests   chan readRequest
	writeRequests  chan writeRequest
	casRequests    chan casRequest
	deleteRequests chan deleteRequest
	keyRequests    chan keyRequest
}

func NewChanDict(file string) (*ChanDict, error) {
	ctx, cancel := context.WithCancel(context.Background())
	readRequests := make(chan readRequest)
	writeRequests := make(chan writeRequest)
	casRequests := make(chan casRequest)
	deleteRequests := make(chan deleteRequest)
	keyRequests := make(chan keyRequest)
	d := &ChanDict{
		readRequests:   readRequests,
		writeRequests:  writeRequests,
		casRequests:    casRequests,
		deleteRequests: deleteRequests,
		keyRequests:    keyRequests,
	}
	// This is a lambda, so we don't have to add members to the struct
	runtime.SetFinalizer(d, func(dict *ChanDict) {
		cancel()
	})

	exists, err := checkAndCreate(file)
	if exists && err != nil {
		cancel()
		return nil, err
	}
	//files
	fk, err := syncfile.NewSyncFile(file+".idx", FileMode)
	if err != nil {
		cancel()
		return nil, err
	}
	fv, err := syncfile.NewSyncFile(file, FileMode)
	if err != nil {
		cancel()
		return nil, err
	}

	// We can't have run be a method of ChanDict, because otherwise then the goroutine will keep the reference alive
	go run(ctx, fk, fv, readRequests, writeRequests, casRequests, deleteRequests, keyRequests)

	return d, nil
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

func run(parentCtx context.Context, fk *syncfile.SyncFile, fv *syncfile.SyncFile,
	readRequests <-chan readRequest, writeRequests <-chan writeRequest, casRequests <-chan casRequest,
	deleteRequests <-chan deleteRequest, keyRequests <-chan keyRequest) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	valDict := make(map[string]*Cmd)
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

	b, _ := fk.ReadFile()
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
	//fmt.Println("read")
	//for _, v := range valDict {
	//fmt.Printf("%+v\n", v)
	//}

	for {
		select {
		case <-ctx.Done():
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
			writeKey(fk, 1, 0, 0, []byte(dr.deleteKey), true, -1)
			close(dr.responseChan)
		case wr := <-writeRequests:
			var err error
			var seek, newSeek int64
			cmd := &Cmd{Size: uint32(len(wr.writeVal))}
			if val, exists := valDict[wr.readKey]; exists {
				if val.Size >= uint32(len(wr.writeVal)) {
					//write at old seek
					_, _, err = fv.WriteAt(wr.writeVal, int64(val.Seek))
				} else {
					//write at new seek
					seek, _, err = fv.Write(wr.writeVal)
					cmd.Seek = uint32(seek)
				}
				if err == nil {
					newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, []byte(wr.readKey), true, int64(cmd.KeySeek))
					cmd.KeySeek = uint32(newSeek)
				}
			} else {
				// new key

				seek, _, err = fv.Write(wr.writeVal)
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
				valDict[wr.readKey] = cmd
			}

			wr.responseChan <- writeRequestResponse{err}
		case rr := <-readRequests:
			if val, exists := valDict[rr.readKey]; exists {
				b, err := fv.Read(int64(val.Size), int64(val.Seek))
				rr.responseChan <- readRequestResponse{b, err}
			} else {
				rr.responseChan <- readRequestResponse{nil, ErrKeyNotFound}
			}

		case kr := <-keyRequests:

			//sort slice
			sort.Slice(keysDict, func(i, j int) bool {
				return bytes.Compare(keysDict[i], keysDict[j]) <= 0
			})
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
			kr.responseChan <- keyRequestResponse{keys: result}
			close(kr.responseChan)
		}
	}
}

func writeKey(fk *syncfile.SyncFile, t uint8, seek, size uint32, key []byte, sync bool, keySeek int64) (newSeek int64, err error) {
	//get buf from pool
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	buf.Grow(14 + len(key))

	//encode
	binary.Write(buf, binary.BigEndian, uint8(0)) //1byte
	binary.Write(buf, binary.BigEndian, t)        //1byte
	binary.Write(buf, binary.BigEndian, seek)     //4byte
	binary.Write(buf, binary.BigEndian, size)     //4byte
	binary.Write(buf, binary.BigEndian, uint32(time.Now().Unix()))
	binary.Write(buf, binary.BigEndian, uint16(len(key)))
	buf.Write(key)

	if sync {
		if keySeek < 0 {
			newSeek, _, err = fk.Write(buf.Bytes())
		} else {
			newSeek, _, err = fk.WriteAt(buf.Bytes(), int64(keySeek))
		}

	} else {
		newSeek, _, err = fk.WriteNoSync(buf.Bytes())
	}

	return newSeek, err
}

// SetKey internal command may be close in interface
func (dict *ChanDict) SetKey(key string, val []byte) error {
	c := make(chan writeRequestResponse)
	w := writeRequest{readKey: key, writeVal: val, responseChan: c}
	dict.writeRequests <- w
	resp := <-c
	return resp.err
}

func (dict *ChanDict) ReadKey(key string) ([]byte, error) {
	c := make(chan readRequestResponse)
	w := readRequest{readKey: key, responseChan: c}
	dict.readRequests <- w
	resp := <-c
	return resp.val, resp.err
}

func (dict *ChanDict) CasVal(key string, oldVal, newVal []byte, setOnNotExists bool) bool {
	c := make(chan bool)
	w := casRequest{readKey: key, oldVal: oldVal, newVal: newVal, responseChan: c, setOnNotExists: setOnNotExists}
	dict.casRequests <- w
	return <-c
}

func (dict *ChanDict) DeleteKey(key string) {
	c := make(chan struct{})
	d := deleteRequest{deleteKey: key, responseChan: c}
	dict.deleteRequests <- d
	<-c
}

func (dict *ChanDict) ReadKeys(from []byte, limit, offset uint32, asc bool) [][]byte {
	c := make(chan keyRequestResponse)
	w := keyRequest{responseChan: c, fromKey: from, limit: limit, offset: offset, asc: asc}

	dict.keyRequests <- w
	resp := <-c
	return resp.keys
}

// Set store val and key
// If key exists and has same or more size - value will be overwriten, else - appended
// If err on insert val - key not inserted
func Set(file string, key []byte, val []byte) (err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	err = db.SetKey(string(key), val)
	return err
}

// Open create file (with dirs) or read keys to map
// Save for multiple open
func Open(file string) (db *ChanDict, err error) {
	mutex.Lock()
	defer mutex.Unlock()

	v, ok := stores[file]
	if ok {
		return v, nil
	}

	//fmt.Println("NewChanDict")
	db, err = NewChanDict(file)
	if err == nil {
		stores[file] = db
	}
	return db, err
}

// Get return value by key or nil and error
func Get(file string, key []byte) (val []byte, err error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}
	val, err = db.ReadKey(string(key))
	return val, err
}

// Keys return keys in asc/desc order (false - descending,true - ascending)
// if limit == 0 return all keys
// offset - skip count records
// If from not nil - return keys after from (from not included)
// If last byte of from == "*" - use as prefix
func Keys(file string, from []byte, limit, offset uint32, asc bool) ([][]byte, error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}
	val := db.ReadKeys(from, limit, offset, asc)
	return val, err
}

// Close close file key and file val and delete db from map
func Close(file string) (err error) {
	_, ok := stores[file]
	if !ok {
		return ErrDbNotOpen
	}
	mutex.Lock()
	delete(stores, file)
	mutex.Unlock()
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
func Gets(file string, keys [][]byte) (result [][]byte) {
	var wg sync.WaitGroup
	var mut = &sync.Mutex{}

	read := func(k []byte) {
		defer wg.Done()
		val, err := Get(file, k)
		if err == nil {
			mut.Lock()
			result = append(result, k)
			result = append(result, val)
			mut.Unlock()
		}
	}

	wg.Add(len(keys))
	for _, key := range keys {
		go read(key)
	}
	wg.Wait()
	return result
}

// Sets store vals and keys like bulk insert
// Fsync will called only twice at end of insertion
func Sets(file string, pairs [][]byte) (err error) {

	for i := range pairs {
		if i%2 != 0 {
			// on even - append val and store key
			if pairs[i] == nil || pairs[i-1] == nil {
				break
			}
			err = Set(file, pairs[i-1], pairs[i])
			if err != nil {
				break
			}
		}
	}
	return err
}

func Delete(file string, key []byte) (deleted bool, err error) {
	db, err := Open(file)
	if err != nil {
		return deleted, err
	}
	db.DeleteKey(string(key))
	return true, err
}
