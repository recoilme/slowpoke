package chandict

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
)

var (
	stores sync.Map
	// ErrKeyNotFound - key not found
	ErrKeyNotFound = errors.New("Error: key not found")
	// ErrDbOpened - db is opened
	ErrDbOpened = errors.New("Error: db is opened")
	// ErrDbNotOpen - db not open
	ErrDbNotOpen = errors.New("Error: db not open")
)

type readRequestResponse struct {
	val    []byte
	exists bool
}

type readRequest struct {
	readKey      string
	responseChan chan readRequestResponse
}

type writeRequest struct {
	readKey      string
	writeVal     []byte
	responseChan chan struct{}
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
	responseChan chan keyRequestResponse
}

type ChanDict struct {
	readRequests   chan readRequest
	writeRequests  chan writeRequest
	casRequests    chan casRequest
	deleteRequests chan deleteRequest
	keyRequests    chan keyRequest
}

func NewChanDict() *ChanDict {
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
	// We can't have run be a method of ChanDict, because otherwise then the goroutine will keep the reference alive
	go run(ctx, readRequests, writeRequests, casRequests, deleteRequests, keyRequests)
	return d
}

func run(parentCtx context.Context, readRequests <-chan readRequest, writeRequests <-chan writeRequest, casRequests <-chan casRequest,
	deleteRequests <-chan deleteRequest, keyRequests <-chan keyRequest) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	localDict := make(map[string][]byte)
	var keysDict [][]byte
	keysDict = make([][]byte, 0)
	for {
		select {
		case <-ctx.Done():
			return
		case dr := <-deleteRequests:
			delete(localDict, dr.deleteKey)
			close(dr.responseChan)
		case wr := <-writeRequests:
			localDict[wr.readKey] = wr.writeVal
			keysDict = append(keysDict, []byte(wr.readKey))
			//_ = s
			//fmt.Println("s", s)
			//keysDict := append(keysDict, []byte(wr.readKey))

			close(wr.responseChan)
		case rr := <-readRequests:
			val, exists := localDict[rr.readKey]
			rr.responseChan <- readRequestResponse{val, exists}
		case cr := <-casRequests:
			if val, exists := localDict[cr.readKey]; exists && bytes.Equal(val, cr.oldVal) {
				localDict[cr.readKey] = cr.newVal

				cr.responseChan <- true
			} else if !exists && cr.setOnNotExists {
				localDict[cr.readKey] = cr.newVal
				cr.responseChan <- true
			} else {
				cr.responseChan <- false

			}
		case kr := <-keyRequests:
			kr.responseChan <- keyRequestResponse{keys: keysDict}
		}
	}
}

func (dict *ChanDict) SetKey(key string, val []byte) {
	c := make(chan struct{})
	w := writeRequest{readKey: key, writeVal: val, responseChan: c}
	dict.writeRequests <- w
	<-c
}

func (dict *ChanDict) ReadKey(key string) ([]byte, bool) {
	c := make(chan readRequestResponse)
	w := readRequest{readKey: key, responseChan: c}
	dict.readRequests <- w
	resp := <-c
	return resp.val, resp.exists
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

func (dict *ChanDict) ReadKeys() [][]byte {
	c := make(chan keyRequestResponse)
	w := keyRequest{responseChan: c}
	//w := readRequest{readKey: key, responseChan: c}
	dict.keyRequests <- w
	//dict.readRequests <- w
	resp := <-c
	return resp.keys
}

// Set store val and key
// If key exists and has same or more size - value will be overwriten, else - appended
// If err on insert val - key not inserted
func Set(file string, key []byte, val []byte) (err error) {
	db, err := Open(file)
	if err != nil {
		return err
	}
	db.SetKey(string(key), val)
	return err
}

// Open create file (with dirs) or read keys to map
// Save for multiple open
func Open(file string) (db *ChanDict, err error) {
	var ok bool
	v, ok := stores.Load(file)
	if ok {
		return v.(*ChanDict), nil
	}
	db = NewChanDict()
	stores.Store(file, db)
	return db, err
}

// Get return value by key or nil and error
func Get(file string, key []byte) (val []byte, err error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}
	val, _ = db.ReadKey(string(key))
	return val, err
}

func Keys(file string) ([][]byte, error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}
	val := db.ReadKeys()
	return val, err
}

// Close close file key and file val and delete db from map
func Close(file string) (err error) {
	_, ok := stores.Load(file)
	if !ok {
		return ErrDbNotOpen
	}
	//err = db.Fkey.Close()
	//err = db.Fval.Close()
	//delete(dbs, file)
	stores.Delete(file)
	return err
}

// CloseAll - close all opened Db
func CloseAll() (err error) {

	stores.Range(func(k, v interface{}) bool {
		err = Close(k.(string))
		if err != nil {
			return false
		}
		return true // if false, Range stops
	})

	return err
}

// DeleteFile close file key and file val and delete db from map and disk
func DeleteFile(file string) (err error) {
	stores.Delete(file)
	//err = os.Remove(file)
	//err = os.Remove(file + ".idx")*/
	return err
}

// Gets return key/value pairs
func Gets(file string, keys [][]byte) (result [][]byte) {
	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}

	read := func(k []byte) {
		defer wg.Done()
		val, err := Get(file, k)
		if err == nil {
			mutex.Lock()
			result = append(result, k)
			result = append(result, val)
			mutex.Unlock()
		}
	}

	wg.Add(len(keys))
	for _, key := range keys {
		go read(key)
	}
	wg.Wait()
	return result
}
