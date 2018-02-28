package main

import (
	"log"
	"testing"
)

func TestNil(t *testing.T) {
	var v []byte
	if v == nil {
		log.Println("dont panic")
	}
	if len(v) == 0 {
		log.Println("dont panic")
	}

}
