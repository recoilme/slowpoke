**Description**

Package slowpoke implements a low-level key/value store on Go standart library. Keys are stored in memory (with persistence), values stored on disk.

**Motivation**

Replace bolt.db with more simple and efficient engine: http://recoilmeblog.tggram.com/post/96

**How it works**

The design is very simple. Keys are stored in memory with persistance too disk. Values stored on disk only.


**Server**

You may found simple http server here: https://github.com/recoilme/slowpoke/tree/master/simpleserver

**Example**

```
package main

import (
	"fmt"

	"github.com/recoilme/slowpoke"
)

func main() {
	// create database
	file := "test/example.db"
	// close all opened database
	defer slowpoke.CloseAll()
	// init key/val
	key := []byte("foo")
	val := []byte("bar")
	//store
	slowpoke.Set(file, key, val)
	// get
	res, _ := slowpoke.Get(file, key)
	//result
	fmt.Println(string(res))
}
```

**Api**

All methods are thread safe. See tests for examples.


**Set** 
store val and key with sync at end

File - may be existing file or new 
If path to file contains dirs - dirs will be created
If val is nil - will store only key


**Get** return value by key or nil and error
// Get will open Db if it closed
// return error if any

**Keys** return keys in ascending  or descending order (false - descending,true - ascending)
// if limit == 0 return all keys
// if offset>0 - skip offset records
// If from not nil - return keys after from (from not included)
// If last byte of from == "*" - return keys with this prefix

**Close** - close Db and free used memory
// It run finalizer and cancel goroutine

**Open** - (call automatically on all commands) - open/create file and read keys to memory


**CloseAll** - close all opened files and remove keys from memory


**DeleteFile** - remove files from disk


**Used libraries**

-

**Status**

Used in production (master branch)


**Benchmark**

```
//macbook 2017 slowpoke vs bolt
//The 100 Set took 19.440075ms to run./19.272079ms
//The 100 Get took 671.343µs to run./211.878µs
//The 100 Sets took 1.139579ms to run./?
//The 100 Keys took 36.214µs to run./?
//The second 100 Keys took 20.632µs to run./?
//The 100 Gets took 206.775µs to run./?
```