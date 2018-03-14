package slowpoke

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"os"
	"strconv"
	"sync"
	"testing"
)

func ch(err error, t *testing.T) {
	if err != nil {
		t.Error(err)
	}
}

func TestSet(t *testing.T) {
	var err error
	//_, err = Open("1.db")
	//ch(err, t)
	defer CloseAll()
	val, err := Get("nodb.db", []byte("1"))
	logg(val)

	err = Set("1.db", []byte("1"), []byte("11"))
	ch(err, t)
	err = Set("1.db", []byte("2"), []byte("22"))
	ch(err, t)

}

func TestGet(t *testing.T) {
	Open("1.db")
	defer Close("1.db")
	res, err := Get("1.db", []byte("2"))
	if err != nil {
		t.Error(err)
	}
	logg("Get:" + string(res))
}

func TestAsync(t *testing.T) {

	file := "la.tmp"
	os.Remove(file)
	Open(file)
	defer Close(file)

	messages := make(chan int)
	readmessages := make(chan string)
	var wg sync.WaitGroup

	append := func(i int) {
		defer wg.Done()
		k := []byte("Key:" + strconv.Itoa(i))
		v := []byte("Val:" + strconv.Itoa(i))
		err := Set(file, k, v)
		ch(err, t)
		messages <- i
	}

	read := func(i int) {
		defer wg.Done()
		k := []byte("Key:" + strconv.Itoa(i))
		v := []byte("Val:" + strconv.Itoa(i))

		b, err := Get(file, k)

		ch(err, t)
		if string(b) != string(v) {
			t.Error("not mutch")
		}
		readmessages <- fmt.Sprintf("read N:%d  content:%s", i, string(b))
	}

	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go append(i)

	}

	go func() {
		for i := range messages {
			_ = i
			//fmt.Println(i)
		}
	}()

	go func() {
		for i := range readmessages {
			fmt.Println(i)
		}
	}()

	wg.Wait()

	for i := 1; i <= 5; i++ {

		wg.Add(1)
		go read(i)
	}
	wg.Wait()

}

func TestDelete(t *testing.T) {
	var err error
	f := "2.db"
	os.Remove(f)
	_, err = Open(f)
	ch(err, t)
	defer Close(f)
	err = Set(f, []byte("1"), []byte("11"))
	ch(err, t)
	err = Set(f, []byte("2"), []byte("22"))
	ch(err, t)
	res, err := Get(f, []byte("2"))
	logg(res)
	deleted, err := Delete(f, []byte("2"))
	logg(deleted)
	if !deleted {
		t.Error("not deleted")
	}
	_, err = Get(f, []byte("2"))
	logg(err)
	Close(f)
	_, err = Open(f)
	ch(err, t)
	_, err = Get(f, []byte("2"))
	logg(err)
	d, _ := Get(f, []byte("1"))
	logg(d)
	Close(f)
}

func TestKeys(t *testing.T) {
	var err error
	f := "keys.db"
	os.Remove(f)
	_, err = Open(f)
	ch(err, t)
	defer Close(f)
	append := func(i int) {

		k := []byte(fmt.Sprintf("%02d", i))
		v := []byte("Val:" + strconv.Itoa(i))
		err := Set(f, k, v)
		ch(err, t)

	}
	for i := 1; i <= 20; i++ {
		append(i)
	}

	res, err := Keys(f, nil, 0, 10, false)
	var s = ""
	for _, r := range res {
		s += string(r)
	}
	logg(s)
	s = ""
	from, err := Keys(f, []byte("10"), 2, 0, true)
	for _, r := range from {
		s += string(r)
	}
	logg(s)
	s = ""
	des, err := Keys(f, []byte("10"), 2, 2, false)
	for _, r := range des {
		s += string(r)
	}
	logg(s)
	if s != "0706" {
		t.Error()
	}
	s = ""
	all, _ := Keys(f, nil, 0, 0, false)
	for _, r := range all {
		s += string(r)
	}
	logg(s)
	if s != "2019181716151413121110090807060504030201" {
		t.Error()
	}

	logg("prefix")
	s = ""
	pref, _ := Keys(f, []byte("2*"), 0, 0, false)
	for _, r := range pref {
		s += string(r)
	}
	logg(s)
	if s != "20" {
		t.Error()
	}
}

func TestAsyncKeys(t *testing.T) {
	var err error
	f := "AsyncKeys.db"
	DeleteFile(f)
	_, err = Open(f)
	ch(err, t)
	defer Close(f)
	var wg sync.WaitGroup
	append := func(i int) {
		defer wg.Done()
		k := []byte(fmt.Sprintf("%02d", i))
		v := []byte("Val:" + strconv.Itoa(i))
		err := Set(f, k, v)
		ch(err, t)

	}
	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go append(i)
	}
	wg.Wait()
	readmessages := make(chan string)

	read := func(i int) {
		defer wg.Done()
		slice, _ := Keys(f, nil, 1, i-1, true)
		var s = ""
		for _, r := range slice {
			s += string(r)
		}
		readmessages <- fmt.Sprintf("read N:%d  content:%s", i, s)
	}

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go read(i)
	}
	go func() {
		for i := range readmessages {
			fmt.Println(i)
		}
	}()

	wg.Wait()
}

func TestRewriteVal(t *testing.T) {
	var err error
	f := "rewrite.db"
	DeleteFile(f)
	_, err = Open(f)
	ch(err, t)
	defer Close(f)

	ch(Set(f, []byte("key1"), []byte("val1")), t)
	ch(Set(f, []byte("key1"), []byte("val2")), t)
	ch(Set(f, []byte("key3"), []byte("val3")), t)
	ch(Set(f, []byte("key1"), []byte("val0")), t)
	ch(Set(f, []byte("key1"), []byte("val")), t)
	v, _ := Get(f, []byte("key1"))
	if !bytes.Equal([]byte("val"), v) {
		fmt.Println(string(v))
		t.Error("not equal")
	}
}

func variadic(a ...[]byte) {
	fmt.Println("len:", len(a))
	for i := range a {
		if i%2 != 0 {
			fmt.Println(a[i-1], a[i])
		}
	}
}

func TestSets(t *testing.T) {
	var err error
	f := "rewrite.db"
	DeleteFile(f)
	_, err = Open(f)
	ch(err, t)

	var a [][]byte
	for i := 0; i < 10; i++ {
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, uint32(i))
		a = append(a, bs)
		a = append(a, bs)
	}
	Sets(f, a...)
	Close(f)
	keys, _ := Keys(f, nil, 5, 0, false)
	logg(keys)
	for _, key := range keys {
		v, _ := Get(f, key)
		logg(v)
	}
	Close(f)
}
