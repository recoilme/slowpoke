package slowpoke

import (
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
	err = Open("1.db")
	ch(err, t)
	defer Close("1.db")
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
	log("Get:" + string(res))
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
	err = Open(f)
	ch(err, t)
	defer Close(f)
	err = Set(f, []byte("1"), []byte("11"))
	ch(err, t)
	err = Set(f, []byte("2"), []byte("22"))
	ch(err, t)
	res, err := Get(f, []byte("2"))
	log(res)
	deleted, err := Delete(f, []byte("2"))
	log(deleted)
	if !deleted {
		t.Error("not deleted")
	}
	_, err = Get(f, []byte("2"))
	log(err)
	Close(f)
	err = Open(f)
	ch(err, t)
	_, err = Get(f, []byte("2"))
	log(err)
	d, _ := Get(f, []byte("1"))
	log(d)
	Close(f)
}
