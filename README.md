[![Build Status](https://travis-ci.org/recoilme/slowpoke.svg?branch=master)](https://travis-ci.org/recoilme/slowpoke) [![Go Report Card](https://goreportcard.com/badge/github.com/recoilme/slowpoke)](https://goreportcard.com/report/github.com/recoilme/slowpoke)
[![Documentation](https://godoc.org/github.com/recoilme/slowpoke?status.svg)](https://godoc.org/github.com/recoilme/slowpoke)

**Description**

Package slowpoke is a simple key/value store written using Go's standard library only. Keys are stored in memory (with persistence), values stored on disk.

Description on russian: https://habr.com/post/354224/

![slowpoke](http://tggram.com/media/recoilme/photos/file_488344.jpg)

**Motivation**

Replace [Bolt](https://github.com/boltdb/bolt) with a simpler and more efficient engine: http://recoilmeblog.tggram.com/post/96

**How it works**

Keys are stored in memory with persistence to disk. Values stored on disk only.

**Slowpoke is parallel**


![](https://habrastorage.org/webt/yp/ok/c3/ypokc33wp-p1jcewq4a221di1hu.png)


**Server**


GRPC Server example: [okdb](https://github.com/recoilme/okdb)

**Complex examples**

[typegram](https://github.com/recoilme/tgram)

zen platform for authors and their subscribers with a minimalistic design and user-friendly interface.

[golang-gin-realworld-example-app](https://github.com/recoilme/golang-gin-realworld-example-app)

This codebase was created to demonstrate a fully fledged fullstack application built with Golang/Gin/Slowpoke including CRUD operations, authentication, routing, pagination, and more.


**Basic example**

```golang
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

**Lazy example**

```golang
func TestGob(t *testing.T) {
	file := "test/gob.db"
	DeleteFile(file)
	defer CloseAll()
	type Post struct {
		Id       int
		Content  string
		Category string
	}

	for i := 0; i < 20; i++ {
		post := &Post{Id: i, Content: "Content:" + strconv.Itoa(i)}
		err := SetGob(file, post.Id, post)
		ch(err, t)
	}

	for i := 0; i < 20; i++ {
		var post = new(Post)
		err := GetGob(file, i, post)
		ch(err, t)
		fmt.Println("i:", i, "Post:", post)
	}
}
```

**Advanced example**

```golang
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

All methods are thread-safe.


- **Set/Sets/SetGob** 

Store val and key with sync at end. If the file does not exist it will be created. 


- **Get/Gets/GetGob** 

Return the value for the given key or nil and an error. `Get` will open the database if necessary.

- **Keys** 

Return keys in ascending/descending order. 

With limit and offset.

If `from` is not nil, return keys lexicographically greater than the `from` value.

If `from` ends with asterix `*`, return keys with the prefix equal to `from` without the asterix.

- **Open/Close** 

`Open` - open/create the file possibly creating appropriate directories, and read keys.

`Close` - close the databse and free used memory.


- **CloseAll** 

Close all files and remove keys from memory


- **DeleteFile** 

Delete files from disk and lose all the data.


**Used libraries**

-

**Status**

Used in production (master branch)


**Benchmark**


[All tests here](https://github.com/recoilme/pogreb-bench)


Some tests, MacBook Pro (Retina, 13-inch, Early 2015)


### Test 1
Number of keys: 1000000
Minimum key size: 16, maximum key size: 64
Minimum value size: 128, maximum value size: 512
Concurrency: 2


|                       | pogreb  | goleveldb | bolt   | badgerdb | pudge  | slowpoke | pudge(mem) |
|-----------------------|---------|-----------|--------|----------|--------|----------|------------|
| 1M (Put+Get), seconds | 187     | 38        | 126    | 34       | 23     | 23       | 2          |
| 1M Put, ops/sec       | 5336    | 34743     | 8054   | 33539    | 47298  | 46789    | 439581     |
| 1M Get, ops/sec       | 1782423 | 98406     | 499871 | 220597   | 499172 | 445783   | 1652069    |
| FileSize,Mb           | 568     | 357       | 552    | 487      | 358    | 358      | 358        |



macbook 2017 slowpoke vs badger


```
//https://github.com/recoilme/datastore_benchmarks

Slowpoke:
Put time: 1.16221931s
Get time: 805.776917ms
Db size: 1 048 570 bytes (Zero bytes overhead)
Index size: 5 033 136 bytes

Badger:
Put time: 902.318742ms
Get time: 723.95486ms
Vlog size: 7 247 634 bytes
Sst size: 6 445 276 bytes
```
