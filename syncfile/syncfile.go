// Package syncfile implements concurrent write to file & read from file
package syncfile

import (
	"bytes"
	"os"
	"sync"
)

// SyncFile helps to append lines to a file.
//
// It is safe for concurrent usage.
type SyncFile struct {
	f  *os.File
	mu *sync.RWMutex
}

// NewSyncFile returns a new SyncFile.
//
// It immediatly opens (and creates) the file.
func NewSyncFile(name string, perm os.FileMode) (*SyncFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, perm)
	if err != nil {
		return nil, err
	}
	return &SyncFile{
		f:  f,
		mu: new(sync.RWMutex),
	}, nil
}

// Append appends the given byte array to the file.
func (sf *SyncFile) Append(b []byte) error {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	_, _ = buf.Write(b)
	return sf.write(buf.Bytes())
}

// Read read size bytes from seek
func (sf *SyncFile) Read(size int64, seek int64) ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	return sf.read(buf, size, seek)
}

func (sf *SyncFile) read(buf *bytes.Buffer, size int64, seek int64) ([]byte, error) {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	var err error
	_, err = sf.f.Seek(seek, 0)
	if err != nil {
		return nil, err
	}
	byteSlice := make([]byte, size)
	buf.Grow(int(size))
	_, err = sf.f.Read(byteSlice)
	//fmt.Println("seek:", ret, "read:", string(byteSlice))
	buf.Write(byteSlice)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sf *SyncFile) write(b []byte) error {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	sf.f.Seek(0, 2)
	_, err := sf.f.Write(b)
	if err != nil {
		return nil
	}
	return sf.f.Sync() // ensure that the write is done.
}

// Close closes the underlying file.
func (sf *SyncFile) Close() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return sf.f.Close()
}

var bufPool = &sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}
