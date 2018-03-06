**Description**

Package slowpoke implements a low-level key/value store in pure Go.
Keys stored in memory (BTree), Value stored on disk.


**Motivation**

Replace bolt.db with more simple and efficient engine: http://recoilmeblog.tggram.com/post/96

**How it work**

Design is very simple. Keys stored in Btree (memory and disk). Vals stored on disk only.

No optimization for ssd/mmap etc. Just files. Just work.


**Server**

You may found simple http server here: https://github.com/recoilme/slowpoke/tree/master/simpleserver

**Example**

```
// Create/open file 1.db.idx and store key: []byte("1")
// Create/open file 1.db and store val: []byte("11")
Set("1.db", []byte("1"), []byte("11"))

// add key 2 and val 22
err = Set("1.db", []byte("2"), []byte("22"))

// get value fo key 2
res, _ := Get("1.db", []byte("2"))
logg(res)

// delete key 2
Delete("1.db", []byte("2"))

// get first 10 keys in descending order 
res, _ := Keys("1.db", nil, 0, 10, false)
var s = ""
for _, r := range res {
  s += string(r)
}
logg(s)

// Close all opened files
CloseAll()
```

**Api**

All methods are thread safe. See tests for examples.


Set - put or replace key/val. Keys stored in memory and in log file (*.idx). Values - on disk only.
If val == nil - stored only keys. Usefull for indexing.


Get - return value by key


Keys - return keys
// Keys return keys in asc/desc order (false - descending,true - ascending)
// if limit == 0 return all keys
// offset - skip count records
// If from not nil - return keys after from (from not included)
// If last byte of from == "*" - use as prefix


Close - close file and remove keys from memory


Open - (call automatically) - open/create file and read keys to memory


CloseAll - close all opened files and remove keys from memory


DeleteFile - remove files from disk


**Used librarys**

github.com/recoilme/syncfile - thread safe read write file
github.com/tidwall/btree - Btree

**Status**

i use it in production (master branch)
