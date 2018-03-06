package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/recoilme/slowpoke"
)

func main() {
	testSet()

}

func testSet() {
	file := "1.db"
	err := slowpoke.DeleteFile(file)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup

	append := func(i int) {
		defer wg.Done()
		k := []byte(fmt.Sprintf("%04d", i))
		err := slowpoke.Set(file, k, k)
		if err != nil {
			fmt.Println(err)
		}
	}

	t1 := time.Now()
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		append(i)
	}
	wg.Wait()
	t2 := time.Now()

	fmt.Printf("The 10000 Set took %v to run.\n", t2.Sub(t1))
	slowpoke.CloseAll()
}

//The 10000 Set took 1.650810209s to run.
