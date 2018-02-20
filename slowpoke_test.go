package main

import (
	"fmt"
	"testing"
)

func TestExample(t *testing.T) {
	InitDatabase()
	err := Set("test", []byte("hello"), []byte("world"))
	if err != nil {
		fmt.Println(err)
	}
	val, err := Get("test", []byte("hello"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(val))
}
