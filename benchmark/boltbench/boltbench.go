package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/recoilme/slowpoke"
)

var boltdb *bolt.DB

func main() {
	testSet()

}

func testSet() {
	var err error
	boltdb, err = bolt.Open("2.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		fmt.Printf("%s\n", err)
	}
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
	boltdb.Close()
	t2 := time.Now()

	fmt.Printf("The 100 Set took %v to run.\n", t2.Sub(t1))
	slowpoke.CloseAll()
}

//The 10000 Set took 1.650810209s to run.
