package main

import (
	"fmt"
	"log"
	"testing"
)

type Store []*KV

func TestStreamEncode(t *testing.T) {

}
func TestStoreKV(t *testing.T) {
	var keys = Store{} //= make([]*KV, 0, 0)
	kv := &KV{Key: []byte("12"), Seek: 2, Size: 3}
	keys = append(keys, kv)
	kv2 := &KV{Key: []byte("123"), Seek: 24, Size: 32}
	keys = append(keys, kv2)

	err := writeGob("./student.gob", keys)
	if err != nil {
		fmt.Println(err)
	}
	var keysr = new(Store) //make([]*KV, 0, 0)
	err2 := readGob("./student.gob", keysr)
	if err2 != nil {
		fmt.Println(err2)
	}
	for _, v := range *keysr {
		fmt.Println(v.Seek, "\t", v.Size)
	}
}

func TestReadGob(t *testing.T) {
	var keys = make([]*KV, 0, 0)
	err := readGob("./student.gob", &keys)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("keys1:%+v", keys[1])
}
func TestOpenCloseEmpty(t *testing.T) {
	err := InitDatabase()
	log.Fatal(err)
	CloseDatabase()
}

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

/*

func TestKeys(t *testing.T) {
	InitDatabase()
	testSize := 20 //minimum 10
	file := "TestKeys"
	for i := 0; i < testSize; i++ {
		// Put int in BigEndian format (for correct sorting)
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(i))
		Set(file, b, b)
	}
	// get all keys in descending order
	keys := Keys(file, 0, 0, false)
	if len(keys) != testSize {
		t.Error(len(keys))
	}
	for i := 0; i < testSize; i++ {
		key := int(binary.BigEndian.Uint32(keys[i]))
		//key will 19 .. 0
		if (i + key) != testSize-1 {
			t.Error()
		}
	}
	//get value by key 18
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(18))
	res, _ := Get(file, b)
	val := int(binary.BigEndian.Uint32(res))
	//it must 18
	if val != 18 {
		t.Error()
	}

	//Keys return keys from file, limit, offset, ascending(true)
	last10 := Keys(file, 10, 0, false)
	fmt.Println(last10) //19-10

	last2 := Keys(file, 2, 2, false)
	fmt.Println(last2) //17,16

	next10 := Keys(file, 20, 10, false)
	fmt.Println(next10) //9-0 (10 значений)

	first2 := Keys(file, 2, 0, true)
	fmt.Println(first2) //0,1

	next3 := Keys(file, 3, 2, true)
	fmt.Println(next3) //2,3,4

	CloseDatabase()
}

/*
func BenchmarkSample(b *testing.B) {
	b.SetBytes(2)
	for i := 0; i < b.N; i++ {
		if x := fmt.Sprintf("%d", 42); x != "42" {
			b.Fatalf("Unexpected string: %s", x)
		}
	}
}
*/
