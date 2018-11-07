package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// FileMode - file will be created in this mode
	FileMode = 0666
	// DirMode - dirs will be created in this mode
	DirMode = 0777
)

var (
	mem struct {
		sync.RWMutex
		kv   map[string][]byte
		keys [][]byte
	}
	stores struct {
		sync.RWMutex
		store map[string]*Db
	}
)

type Db struct {
	fk   *os.File
	fv   *os.File
	keys [][]byte
	vals map[string]*Cmd
}

type Cmd struct {
	Seek    uint32
	Size    uint32
	KeySeek uint32
}

func init() {
	mem.kv = make(map[string][]byte)
	mem.keys = make([][]byte, 0)
	stores.store = make(map[string]*Db)
}

func main() {
	Set("1", 1, 2)

	var v int64
	Get("1", 1, &v)
	log.Printf("%T %+v\n", v, v)
}

func toBinary(v interface{}) ([]byte, error) {
	var err error

	buf := new(bytes.Buffer)
	switch v.(type) {
	case bool, float32, float64, complex64, complex128, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, []byte:
		err = binary.Write(buf, binary.BigEndian, v)
	case int:
		i64 := int64(v.(int))
		err = binary.Write(buf, binary.BigEndian, i64)
	case string:
		_, err = buf.Write([]byte((v.(string))))
	default:
		enc := gob.NewEncoder(buf)
		err = enc.Encode(v)
	}
	log.Println("buf", buf.Bytes(), string(buf.Bytes()))
	return buf.Bytes(), err
}

func set(f string, key, val []byte) (err error) {
	if f == "" {
		mem.Lock()
		defer mem.Unlock()
		mem.kv[string(key)] = val
	} else {
		//file db
		stores.Lock()
		defer stores.Unlock()
		db, ok := stores.store[f]
		if !ok {
			//new db?
			db, err = newDb(f)
			if err != nil {
				return err
			}
			//log.Println("db", db)
			stores.store[f] = db
		}

		oldCmd, exists := db.vals[string(key)]
		log.Println("val", val)
		cmd, err := writeKeyVal(db.fk, db.fv, key, val, exists, oldCmd)
		if err != nil {
			return err
		}
		db.vals[string(key)] = cmd
	}
	return err
}

func writeKeyVal(fk, fv *os.File, readKey, writeVal []byte, exists bool, oldCmd *Cmd) (cmd *Cmd, err error) {

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
	buf := new(bytes.Buffer)
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

func Set(f string, params ...interface{}) error {
	var e error
	for k, v := range params {
		if k%2 != 0 {
			key, err := toBinary(params[k-1])
			if err != nil {
				e = err
				break
			}
			val := new(bytes.Buffer)
			switch v.(type) {
			case []byte:
				b, err := toBinary(v)
				if err != nil {
					e = err
					break
				}
				val.Write(b)
			default:
				err = gob.NewEncoder(val).Encode(v)
				if err != nil {
					e = err
					break
				}
			}
			return set(f, key, val.Bytes())

		}
	}
	return e
}

func newDb(f string) (db *Db, err error) {
	db = &Db{}
	db.keys = make([][]byte, 0)
	db.vals = make(map[string]*Cmd)

	_, err = os.Stat(f)
	if err != nil {
		// file not exists - create dirs if any
		if os.IsNotExist(err) {
			if filepath.Dir(f) != "." {
				err = os.MkdirAll(filepath.Dir(f), DirMode)
				if err != nil {
					return nil, err
				}
			}
		} else {
			return nil, err
		}
	}
	db.fk, db.fv, err = openFiles(f)
	if err != nil {
		return nil, err
	}
	//TODO read logs
	return db, err
}

func openFiles(f string) (fk, fv *os.File, err error) {
	//files
	fk, err = os.OpenFile(f+".idx", os.O_CREATE|os.O_RDWR, FileMode)
	if err != nil {
		return nil, nil, err
	}
	fv, err = os.OpenFile(f, os.O_CREATE|os.O_RDWR, FileMode)
	if err != nil {
		return nil, nil, err
	}
	return fk, fv, err
}

func Get(f string, k interface{}, v interface{}) (err error) {
	b := make([]byte, 0)
	key, err := toBinary(k)
	if err != nil {
		return err
	}

	if f == "" {
		mem.RLock()
		defer mem.RUnlock()
		b = mem.kv[string(key)]
	} else {
		//file db
		stores.RLock()
		defer stores.RUnlock()
		db, ok := stores.store[f]
		if !ok {
			//new db?
			log.Println("newdb")
			db, err = newDb(f)
			if err != nil {
				return err
			}
		}
		if val, exists := db.vals[string(key)]; exists {
			//fmt.Printf("rr:%s %+v\n", rr.readKey, val)
			bb := make([]byte, val.Size)
			_, err := db.fv.ReadAt(bb, int64(val.Seek))
			if err != nil {
				return err
			}
			b = bb
		}
	}

	var buf = new(bytes.Buffer)
	switch v.(type) {
	case *[]byte:
		err = gob.NewEncoder(buf).Encode(b)
		if err != nil {
			return err
		}
		err = gob.NewDecoder(buf).Decode(v)
		if err != nil {
			return err
		}
	default:
		buf.Write(b)
		err = gob.NewDecoder(buf).Decode(v)
		if err != nil {
			return err
		}
	}
	return err
}
