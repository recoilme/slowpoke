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

	appendd := func(i int) {
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
		appendd(i)
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

	//Sets
	var pairs [][]byte
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		pairs = append(pairs, k)
		pairs = append(pairs, k)
	}
	t5 := time.Now()
	slowpoke.Sets(file, pairs)
	t6 := time.Now()
	fmt.Printf("The 100 Sets took %v to run.\n", t6.Sub(t5))

	t7 := time.Now()
	slowpoke.Keys(file, nil, 0, 0, true)
	t8 := time.Now()
	fmt.Printf("The 100 Keys took %v to run.\n", t8.Sub(t7))
	slowpoke.CloseAll()
}

//macbook 2017 slowpoke/bolt
//The 100 Set took 13.270801ms to run./15.538641ms
//The 100 Get took 279.128µs to run./191.673µs
//The 100 Sets took 1.124931ms to run./-
//The 100 Keys took 8.583µs to run./-

//Hetzner raid hdd slowpoke/bolt
//The 100 Set took 7.057072837s to run./2.602835939s to run.
//The 100 Get took 275.011µs to run./268.707µs to run.
//The 100 Sets took 53.058325ms to run./-
//The 100 Keys took 16.072µs to run./-
