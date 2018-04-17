[![Build Status](https://travis-ci.org/recoilme/slowpoke.svg?branch=master)](https://travis-ci.org/recoilme/slowpoke) [![Go Report Card](https://goreportcard.com/badge/github.com/recoilme/slowpoke)](https://goreportcard.com/report/github.com/recoilme/slowpoke)
[![Documentation](https://godoc.org/github.com/recoilme/slowpoke?status.svg)](https://godoc.org/github.com/recoilme/slowpoke)

**Description**

Package slowpoke implements a low-level key/value store on Go standard library. Keys are stored in memory (with persistence), values stored on disk.

![slowpoke](http://tggram.com/media/recoilme/photos/file_488344.jpg)

**Motivation**

Replace bolt.db with more simple and efficient engine: http://recoilmeblog.tggram.com/post/96

**How it works**

The design is very simple. Keys are stored in memory with persistence on disk. Values stored on disk only.


**Server**

You may found simple http server here: https://github.com/recoilme/slowpoke/tree/master/simpleserver

GRPC Server (in development): https://github.com/recoilme/okdb

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

**Advanced**

```
type Post struct {
	Id       int
	Content  string
	Category string
}

func main() {
	posts := "test/posts"
	tags := "test/tags"
	var pairs [][]byte
	for i := 0; i < 40; i++ {
		id := make([]byte, 4)
		binary.BigEndian.PutUint32(id, uint32(i))
		post := &Post{Id: i, Content: "Content:" + strconv.Itoa(i), Category: "Category:" + strconv.Itoa(i/10)}
		b, _ := json.Marshal(post)
		pairs = append(pairs, id)
		pairs = append(pairs, b)
		tag := fmt.Sprintf("%s:%08d", strconv.Itoa(i/10), i)
		//store only tags keys
		slowpoke.Set(tags, []byte(tag), nil)
	}
	//store posts fast
	slowpoke.Sets(posts, pairs)

	//get last 2 post key with offset 2
	limit := uint32(2)
	offset := uint32(2)
	order := false //desc
	keys, _ := slowpoke.Keys(posts, nil, limit, offset, order)
	fmt.Println(keys) //[[0 0 0 37] [0 0 0 36]]

	//get key/ values
	res := slowpoke.Gets(posts, keys)
	for k, v := range res {
		if k%2 == 0 {
			fmt.Print(binary.BigEndian.Uint32(v))
		} else {
			var p Post
			json.Unmarshal(v, &p)
			fmt.Println(p)
		}
	}
	//37{37 Content:37 Category:3}
	//36{36 Content:36 Category:3}

	//free from memory
	slowpoke.Close(posts)
	slowpoke.Close(tags)

	//open Db and read tags by prefix 2:* in ascending order
	tagsKeys, _ := slowpoke.Keys(tags, []byte("2:*"), 0, 0, true)
	for _, v := range tagsKeys {
		fmt.Print(string(v) + ", ")
	}
	//2:00000020, 2:00000021, 2:00000022, 2:00000023, 2:00000024, 2:00000025, 2:00000026, 2:00000027, 2:00000028, 2:00000029,
}
```

**Api**

All methods are thread safe. See tests for examples.


- **Set** 

store val and key with sync at end

File - may be existing file or new 

If path to file contains dirs - dirs will be created

If val is nil - will store only key


- **Get** 

return value by key or nil and error

Get will open Db if it closed

return error if any

- **Keys** 

return keys in ascending  or descending order (false - descending,true - ascending)

If limit == 0 return all keys

If offset>0 - skip offset records

If from not nil - return keys after from (from not included)

If last byte of from == "*" - return keys with this prefix

- **Close** 

close Db and free used memory

It run finalizer and cancel goroutine

- **Open** 

(call automatically on all commands) - open/create file and read keys to memory


- **CloseAll** 

close all opened files and remove keys from memory


- **DeleteFile** 

delete files from disk (all data will be lost!)


**Used libraries**

no

**Status**

Used in production (master branch)


**Benchmark**

```
//macbook 2017 slowpoke vs bolt
//The 100 Set took 19.440075ms to run./19.272079ms
//The 100 Sets took 1.139579ms to run./?
//The 100 Get took 671.343µs to run./211.878µs
//The 100 Gets took 206.775µs to run./?
//The 100 Keys took 36.214µs to run./?
//The second 100 Keys took 5.632µs to run./?
```