package chandict

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestImplementation(t *testing.T) {
	d := NewChanDict()
	d.SetKey("foo", []byte("bar"))
	val, ok := d.ReadKey("foo")
	fmt.Println(string(val))
	d.DeleteKey("foo")
	_, ok = d.ReadKey("foo")
	fmt.Println(ok)
}

func TestOpen(t *testing.T) {
	d, _ := Open("1")
	//fmt.Println(d)
	Set("1", []byte("foo"), []byte("bar"))
	val, ok := d.ReadKey("foo")
	fmt.Println(val)
	d.DeleteKey("foo")
	_, ok = d.ReadKey("foo")
	fmt.Println(ok)
}

func TestAsync(t *testing.T) {
	len := 5
	file := "1"

	messages := make(chan int)
	readmessages := make(chan string)
	var wg sync.WaitGroup

	append := func(i int) {
		defer wg.Done()
		k := ("Key:" + strconv.Itoa(i))
		v := ("Val:" + strconv.Itoa(i))
		Set(file, []byte(k), []byte(v))
		messages <- i
	}

	read := func(i int) {
		defer wg.Done()
		k := ("Key:" + strconv.Itoa(i))
		v := ("Val:" + strconv.Itoa(i))

		b, _ := Get(file, []byte(k))

		if string(b) != string(v) {
			t.Error("not mutch")
		}
		readmessages <- fmt.Sprintf("read N:%d  content:%s", i, string(b))
	}

	for i := 1; i <= len; i++ {
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

	for i := 1; i <= len; i++ {

		wg.Add(1)
		go read(i)
	}
	wg.Wait()

}

func TestBytesConvert(t *testing.T) {
	for i := 1; i <= 20; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(i))
		Set("", b, b)
		bb, _ := Get("", b)
		fmt.Println(binary.BigEndian.Uint32(bb))
	}

	fmt.Println(Keys(""))
}
