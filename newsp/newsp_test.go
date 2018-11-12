package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemBin(t *testing.T) {
	b := []byte("Hello")
	Set("", 1, b)
	var res []byte
	Get("", 1, &res)
	if string(res) != "Hello" {
		t.Error("Not bin", res, &res)
	}

	Set("", []byte("hello"), []byte("world!"))
	Get("", []byte("hello"), &res)
	if string(res) != "world!" {
		t.Error("Not bin", res, &res)
	}

	Set("", uint32(18), 427)
	var i int
	e := Get("", uint32(18), &i)

	if i != 427 {
		t.Error("Not 427", e)
	}
}

func TestSet(t *testing.T) {
	var cnt = 1000000
	file := "test/bench.db"
	//err := DeleteFile(file)
	//if err != nil {
	//fmt.Println(err)
	//}
	var wg sync.WaitGroup

	appendd := func(i int) {
		defer wg.Done()
		k := []byte(fmt.Sprintf("%04d", i))
		err := Set(file, k, k)
		if err != nil {
			fmt.Println(err)
		}
	}

	t1 := time.Now()
	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go appendd(i)
	}
	wg.Wait()
	t2 := time.Now()

	fmt.Printf("The %d Set took %v to run.\n", cnt, t2.Sub(t1))
}

func TestGet(t *testing.T) {
	var cnt = 1000000
	file := "test/bench.db"
	var wg sync.WaitGroup

	read := func(i int) {
		defer wg.Done()
		k := []byte(fmt.Sprintf("%04d", i))
		var res []byte
		_ = Get(file, k, &res)
		if string(res) != string(k) {
			t.Error("Not eq")
		}
		//fmt.Println(string(res))

	}
	wg.Add(1)
	read(0)
	wg.Wait()
	//_ = read
	t3 := time.Now()
	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go read(i)
		//k := []byte(fmt.Sprintf("%04d", i))
		//_, _ = Get(file, k)
	}
	wg.Wait()
	t4 := time.Now()

	fmt.Printf("The %d Get took %v to run.\n", cnt, t4.Sub(t3))

	// даём время горутине финализатора отработать
	//time.Sleep(10 * time.Millisecond)
}

/*
func TestBase(t *testing.T) {
	b := []byte{0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40}
	type Tmp struct {
		F1 string
		F2 int8
		f3 int16
	}
	tmpw := &Tmp{F1: "s", F2: 3, f3: 5}
	var data = []interface{}{
		uint16(61374),
		int8(-54),
		uint8(254),
		true,
		"str",
		b,
		[]byte("45"),
		float32(1.2),
		tmpw,
	}
	for _, v := range data {
		e := Set("", v, v)
		if e != nil {
			t.Error(e)
		}
	}
	f := uint16(61374)
	e := Get("", f, &f)
	if e != nil {
		t.Error(e)
	}
	if f != uint16(61374) {
		t.Error("not eq")
	}

	bb := b
	var bbb []byte
	e = Get("", bb, &bbb)
	if e != nil {
		t.Error(e)
	}
	if bytes.Compare(bb, bbb) != 0 {
		t.Error("not eq")
	}
}
*/
