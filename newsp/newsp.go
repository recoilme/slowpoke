package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"log"
	"sync"
)

var (
	mem struct {
		sync.RWMutex
		kv map[string][]byte
	}
)

func init() {
	mem.kv = make(map[string][]byte)
}

func main() {
	e := set("", 1, 2, 3, 4, "5", "6")
	log.Println(e)
	var a []byte
	a, _ = get("", 1)
	log.Println(a)
	a, _ = get("", 3)
	log.Println(a)
	a, _ = get("", "54")
	log.Println(a)
	if a == nil {
		log.Println("nil")
	}
}

func toBinary(v interface{}) ([]byte, error) {
	var err error
	buf := new(bytes.Buffer)
	switch v.(type) {
	case bool, float32, float64, complex64, complex128, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, []byte:
		err = binary.Write(buf, binary.BigEndian, v)
	case int:
		err = binary.Write(buf, binary.BigEndian, int64(v.(int)))
	case string:
		_, err = buf.Write([]byte((v.(string))))
	default:
		enc := gob.NewEncoder(buf)
		err = enc.Encode(v)
	}
	return buf.Bytes(), err
}

func set(f string, params ...interface{}) error {
	var e error
	if f == "" {
		mem.Lock()
		defer mem.Unlock()
		for k, v := range params {
			if k%2 != 0 {
				key, err := toBinary(params[k-1])
				if err != nil {
					e = err
					break
				}
				val, err := toBinary(v)
				if err != nil {
					e = err
					break
				}
				mem.kv[string(key)] = val
			}
		}
	}
	return e
}

func get(f string, k interface{}) (v []byte, err error) {
	if f == "" {
		mem.RLock()
		defer mem.RUnlock()
		key, err := toBinary(k)
		if err != nil {
			return nil, err
		}
		v = mem.kv[string(key)]
	}
	return v, err
}
