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
		kv   map[string][]byte
		keys [][]byte
	}
)

func init() {
	mem.kv = make(map[string][]byte)
	mem.keys = make([][]byte, 0)
}

func main() {
	e := Set("", 1, 2)
	log.Println(e)
	var v int64
	e = Get("", 1, &v)
	log.Printf("%T %+v\n", v, v)
	//a, _ = get("", 3)
	//log.Println(a)
	//a, _ = get("", "54")
	//log.Println(a)
	//if a == nil {
	//log.Println("nil")
	//}
}

func toBinary(v interface{}) ([]byte, error) {
	var err error

	buf := new(bytes.Buffer)
	switch v.(type) {
	case bool, float32, float64, complex64, complex128, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, []byte:
		err = binary.Write(buf, binary.BigEndian, v)
	case int:
		//log.Printf("set:%T\n", )
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

func Set(f string, params ...interface{}) error {
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
				mem.kv[string(key)] = val.Bytes()
			}
		}
	}
	return e
}

func Get(f string, k interface{}, v interface{}) (err error) {
	if f == "" {
		mem.RLock()
		defer mem.RUnlock()

		key, err := toBinary(k)
		if err != nil {
			return err
		}
		b := mem.kv[string(key)]
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

	}
	return err
}
