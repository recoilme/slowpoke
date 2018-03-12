package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

var boltdb *bolt.DB

func main() {
	file := "1.db"
	var err error
	os.Remove(file)
	boltdb, err = bolt.Open(file, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		fmt.Printf("%s\n", err)
	}
	testSet()

}

func testSet() {
	var err error
	var wg sync.WaitGroup

	append := func(i int) {
		defer wg.Done()
		k := []byte(fmt.Sprintf("%04d", i))
		bucketstr := "1"
		err = boltdb.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucketstr))
			if err != nil {
				return err
			}
			e := b.Put(k, k)
			return e
		})
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
		bucketstr := "1"
		boltdb.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketstr))
			if b == nil {
				return nil
			}
			_ = b.Get(k)
			return nil
		})
	}

	t3 := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		read(i)
	}
	wg.Wait()
	t4 := time.Now()

	fmt.Printf("The 100 Get took %v to run.\n", t4.Sub(t3))
	boltdb.Close()
}

//Macbook 2017
//The 100 Set took 15.538641ms to run.
//The 100 Get took 191.673µs to run.

//Hetzner raid hdd
//The 100 Set took 2.602835939s to run.
//The 100 Get took 268.707µs to run.
