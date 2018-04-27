package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/recoilme/slowpoke"
)

type Post struct {
	Id       int
	Content  string
	Category string
}

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
	advanced()
	gobExample()
}

func advanced() {
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

func gobExample() {
	file := "test/gob"
	i := 1

	// init encoder, decoder
	bufIn := bytes.Buffer{}
	bufOut := bytes.Buffer{}
	enc := gob.NewEncoder(&bufIn)
	dec := gob.NewDecoder(&bufOut)

	//create post
	postIn := &Post{Id: i, Content: "Content:" + strconv.Itoa(i)}
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(i))

	//encode post
	defer slowpoke.Close(file)
	if err := enc.Encode(&postIn); err == nil {
		// store post 2 slowpoke
		err = slowpoke.Set(file, key, bufIn.Bytes())
		if err != nil {
			log.Fatal(err)
		}
	}

	//decode post
	postOut := &Post{}
	if bin, err := slowpoke.Get(file, key); err == nil {
		bufOut.Write(bin)
		if err := dec.Decode(&postOut); err == nil {
			fmt.Println("Post out:")
			fmt.Println("Content:", postOut.Content, "Id:", postOut.Id)
		}
	}

}
