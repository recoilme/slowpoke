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
		fmt.Println(err)
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
	for i := 0; i < 100; i++ {
		wg.Add(1)
		append(i)
	}
	wg.Wait()
	t2 := time.Now()

	fmt.Printf("The 100 Set took %v to run.\n", t2.Sub(t1))

	read := func(i int) {
		defer wg.Done()
		k := []byte(fmt.Sprintf("%04d", i))

		_, _ = slowpoke.Get(file, k)

	}

	t3 := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		read(i)
	}
	wg.Wait()
	t4 := time.Now()

	fmt.Printf("The 100 Get took %v to run.\n", t4.Sub(t3))

	slowpoke.CloseAll()
}

//macbook 2017 slowpoke/bolt
//The 100 Set took 13.270801ms to run./15.538641ms
//The 100 Get took 279.128µs to run./191.673µs to run.

//Hetzner raid hdd slowpoke/bolt
//The 100 Set took 7.057072837s to run./2.602835939s to run.
//The 100 Get took 275.011µs to run./268.707µs to run.
