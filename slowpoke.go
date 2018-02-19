package main

import (
	"fmt"

	"github.com/tidwall/btree"
)

type Item struct {
	File          string
	ID, Pos, Size int64
}

/*
func (i1 *Item) Less(item btree.Item, ctx interface{}) bool {
	i2 := item.(*Item)
	switch tag := ctx.(type) {
	case string:
		if tag == "vals" {
			if i1.Val < i2.Val {
				return true
			} else if i1.Val > i2.Val {
				return false
			}
			// Both vals are equal so we should fall though
			// and let the key comparison take over.
		}
	}
	return i1.Key < i2.Key
}*/

func (i1 *Item) Less(item btree.Item, ctx interface{}) bool {
	i2 := item.(*Item)
	return i1.ID < i2.ID
}

func main() {

	// Create a tree for keys and a tree for values.
	// The "keys" tree will be sorted on the Keys field.
	// The "values" tree will be sorted on the Values field.
	keys := btree.New(16, "keys")

	// Create some items.
	users := []*Item{
		&Item{File: "user", ID: 1},
		&Item{File: "user", ID: 2},
		&Item{File: "user", ID: 10},
	}

	// Insert each user into both trees
	for _, user := range users {
		keys.ReplaceOrInsert(user)
	}

	// Iterate over each user in the key tree
	keys.Descend(func(item btree.Item) bool {
		kvi := item.(*Item)
		fmt.Printf("%s %d\n", kvi.File, kvi.ID)
		return true
	})

}
