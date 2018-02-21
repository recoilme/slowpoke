package slowpoke

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestHelloWorld(t *testing.T) {
	InitDatabase()
	world := "world"
	err := Set("test", []byte("hello"), []byte(world))
	if err != nil {
		t.Error(err)
	}
	val, err := Get("test", []byte("hello"))
	if err != nil {
		fmt.Println(err)
	}
	if string(val) != world {
		t.Error()
	}
	CloseDatabase()
}

func TestKeys(t *testing.T) {
	InitDatabase()
	testSize := 100000
	file := "TestKeys"
	for i := 0; i < testSize; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(i))
		Set(file, b, b)
	}
	keys := Keys(file, 0, 0)
	//fmt.Println("return keys", keys)
	if len(keys) != testSize {
		t.Error(len(keys))
	}
	//for _, v := range keys {
	//key := int(binary.BigEndian.Uint32(v))
	//fmt.Println(key)
	//}
	CloseDatabase()
}
