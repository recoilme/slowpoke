**Description**

Package slowpoke implements a low-level key/value store on pure Go. Keys are stored in memory (with persistance), values stored on disk.

**Motivation**

Replace bolt.db with more simple and efficient engine: http://recoilmeblog.tggram.com/post/96

**How it work**

Design is very simple. Keys are stored in Btree (memory and disk). Values stored on disk only.


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
```
// Keys return keys in asc/desc order (false - descending,true - ascending)
// if limit == 0 return all keys
// offset - skip count records
// If from not nil - return keys after from (from not included)
// If last byte of from == "*" - use as prefix
```

Close - close file and remove keys from memory


Open - (call automatically) - open/create file and read keys to memory


CloseAll - close all opened files and remove keys from memory


DeleteFile - remove files from disk


**Used librarys**

github.com/recoilme/syncfile - thread safe read write file

github.com/tidwall/btree - Btree

**Status**

i use it in production (master branch)


**Async read example**

```
func TestAsyncKeys(t *testing.T) {
	var err error
	f := "AsyncKeys.db"
	DeleteFile(f)
	_, err = Open(f)
	ch(err, t)
	defer Close(f)
	append := func(i int) {

		k := []byte(fmt.Sprintf("%02d", i))
		v := []byte("Val:" + strconv.Itoa(i))
		err := Set(f, k, v)
		ch(err, t)

	}
	for i := 1; i <= 20; i++ {
		append(i)
	}

	readmessages := make(chan string)
	var wg sync.WaitGroup

	read := func(i int) {
		defer wg.Done()
		slice, _ := Keys(f, nil, 1, i-1, true)
		var s = ""
		for _, r := range slice {
			s += string(r)
		}
		readmessages <- fmt.Sprintf("read N:%d  content:%s", i, s)
	}

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go read(i)
	}
	go func() {
		for i := range readmessages {
			fmt.Println(i)
		}
	}()

	wg.Wait()
}

//output
/*
read N:5  content:05
read N:4  content:04
read N:9  content:09
read N:10  content:10
read N:2  content:02
read N:3  content:03
read N:8  content:08
read N:6  content:06
read N:1  content:01
read N:7  content:07
*/
```

**Benchmark**

```
//macbook 2017 slowpoke/bolt
//The 100 Set took 13.270801ms to run./15.538641ms
//The 100 Get took 279.128µs to run./191.673µs
//The 100 Sets took 1.124931ms to run./-
//The 100 Keys took 8.583µs to run./-

//Hetzner raid hdd slowpoke/bolt
//The 100 Set took 7.057072837s to run./2.602835939s to run.
//The 100 Get took 275.011µs to run./268.707µs to run.
//The 100 Sets took 53.058325ms to run./-
//The 100 Keys took 16.072µs to run./-
```