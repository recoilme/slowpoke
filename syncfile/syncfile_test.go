package syncfile

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
)

var sf *SyncFile

func ch(err error) {
	if err != nil {
		panic(err)
	}
}
func TestAppend(t *testing.T) {
	var err error
	file := "la.tmp"
	os.Remove(file)
	sf, err = NewSyncFile(file, 0666)
	ch(err)

	sf.Append([]byte("1234567890"))
	messages := make(chan int)
	readmessages := make(chan string)
	var wg sync.WaitGroup

	append := func(i int) {
		defer wg.Done()
		s := strconv.Itoa(i)
		sf.Append([]byte(s))
		messages <- i
	}

	read := func(i int) {
		defer wg.Done()

		b, err := sf.Read(3, 0)
		//content, err := ioutil.ReadFile(file)
		if err != nil {
			t.Error(err)
		}

		readmessages <- fmt.Sprintf("read N:%d  content:%s", i, string(b))
	}

	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go append(i)
		wg.Add(1)
		go read(i)
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
	sf.Close()
}
