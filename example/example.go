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
