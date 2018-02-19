package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/tidwall/btree"
)

var (
	trees = make(map[string]*btree.BTree)
)

type Object struct {
	FileName string
	Key      []byte
}

func (i1 *Object) Less(item btree.Item, ctx interface{}) bool {
	i2 := item.(*Object)
	if bytes.Compare(i1.Key, i2.Key) < 0 {
		return true
	}
	return false
}

func main() {

	users := []*Object{
		&Object{Key: []byte("a"), FileName: "user"},
		&Object{Key: []byte("aa"), FileName: "user"},
		&Object{Key: []byte("b"), FileName: "user"},
		&Object{Key: []byte("b"), FileName: "msg"},
	}

	for _, o := range users {
		if Store(o) {
			log.Println("inserted")
		} else {
			log.Println("replaced")
		}
	}
	log.Println("msgs asc")
	msgs := getTree("msg")
	msgs.Ascend(iterator)

	log.Println("users desc")
	u := getTree("user")
	u.Descend(iterator)

	log.Println("user a:")
	user_a := &Object{Key: []byte("a"), FileName: "user"}
	item := u.Get(user_a).(*Object)
	fmt.Println(item.FileName, string(item.Key))
}

// Store adds the given item to the tree.
// If tree not exists it will be created
// If an item in the tree already equals the given one, it is removed from the tree and returned false.
// Otherwise, true is returned.
//
// nil cannot be added to the tree (will panic).
func Store(o *Object) bool {
	t := getTree(o.FileName)
	return t.ReplaceOrInsert(o) == nil
}

func getTree(file string) (t *btree.BTree) {
	var ok bool
	t, ok = trees[file]
	if !ok {
		t = btree.New(16, nil)
		trees[file] = t
	}

	return t
}

func iterator(item btree.Item) bool {
	kvi := item.(*Object)
	fmt.Printf("iterator: %s \n", string(kvi.Key))
	return true
}
