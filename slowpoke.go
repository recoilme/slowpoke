package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tidwall/btree"
)

var (
	trees = make(map[string]*btree.BTree)
)

type KV struct {
	Key   []byte
	Value []byte
}

func (i1 *KV) Less(item btree.Item, ctx interface{}) bool {
	i2 := item.(*KV)
	if bytes.Compare(i1.Key, i2.Key) < 0 {
		return true
	}
	return false
}

func main() {
	if e := Set("users", []byte("a")); e != nil {
		panic(e)
	}
	Set("users", []byte("aa"))
	Set("users", []byte("b"))
	Set("users", []byte("bb"))
	u := getTree("users")
	u.Descend(iterator)

	for i := 8; i <= 12; i++ {
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, uint32(i))
		Set("userids", bs)
	}
	userids := getTree("userids")
	userids.Descend(func(item btree.Item) bool {
		kvi := item.(*KV)
		k := binary.BigEndian.Uint32(kvi.Key)
		fmt.Printf("Descend: %d \n", k)
		return true
	})

	userids.Ascend(func(item btree.Item) bool {
		kvi := item.(*KV)
		k := binary.BigEndian.Uint32(kvi.Key)

		fmt.Printf("Ascend: %d \n", k)
		return true
	})

	/*output:
	iterator: bb
	iterator: b
	iterator: aa
	iterator: a
	Descend: 12
	Descend: 11
	Descend: 10
	Descend: 9
	Descend: 8
	Ascend: 8
	Ascend: 9
	Ascend: 10
	Ascend: 11
	Ascend: 12
	*/
}

// Set adds the given key to the tree.
// If tree not exists it will be created
// If an item in the tree already equals the given one, it is removed from the tree and inserted.
//
// nil cannot be added to the tree (will error).
func Set(file string, key []byte) error {
	if key == nil {
		return errors.New("key is nil")
	}
	t := getTree(file)
	if t == nil {
		return errors.New("tree is nil")
	}
	t.ReplaceOrInsert(&KV{Key: key})
	return nil
}

func getTree(file string) (t *btree.BTree) {

	t = trees[file]
	if t == nil {
		t = btree.New(16, nil)
		trees[file] = t
	}

	return t
}

func iterator(item btree.Item) bool {
	kvi := item.(*KV)
	fmt.Printf("iterator: %s \n", string(kvi.Key))
	return true
}
