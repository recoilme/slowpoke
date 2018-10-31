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
	e := set("", int8(1), int8(2), int8(3), int8(4))
	log.Println(e)
	get("", 1)
}

func toBinary(v interface{}) ([]byte, error) {
	var err error
	buf := new(bytes.Buffer)
	switch v.(type) {
	case bool, float32, float64, complex64, complex128, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, []byte:
		err = binary.Write(buf, binary.BigEndian, v)
	case string:
		_, err = buf.Write([]byte(string(v.(string))))
	default:
		enc := gob.NewEncoder(buf)
		err = enc.Encode(v)
	}
	return buf.Bytes(), err
}

func set(f string, params ...interface{}) (err error) {
	if f == "" {
		mem.Lock()
		defer mem.Unlock()

		for k, v := range params {
			if k%2 != 0 {
				key, err := toBinary(params[k-1])
				if err != nil {
					break
				}
				val, err := toBinary(v)
				if err != nil {
					break
				}
				log.Printf("%+v", mem.kv)
				mem.kv[string(key)] = val
				log.Printf("%+v", mem.kv)
			}
		}
		for key, val := range mem.kv {
			log.Println("11111", key, val)
		}
		//log.Printf("%+v", mem.kv)
	}
	return err
}

func get(f string, k interface{}) (err error) {

	if f == "" {
		mem.RLock()
		defer mem.RUnlock()
		key, err := toBinary(k)
		//log.Println(key, err)
		if err != nil {
			return err
		}
		//log.Println("333")
		v := mem.kv[string(key)]
		_ = v
		log.Println(v)
	}
	return err
}
