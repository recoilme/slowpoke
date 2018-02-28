// Package contains handlers for working with boltdb/slowpoke.
// Handler server/database/store/
// database - bolt or slowpoke
// store - bucket for bolt, file for slowpoke
// designed for storing simple strings in keys

/*
Examples

PUT:

# params
host/database/store/key
and value in body

curl -X PUT -H "Content-Type: application/octet-stream" --data-binary "@durov.jpg" localhost:5000/bolt/images/durov
curl -X PUT -H "Content-Type: text/html" -d '{"username":"xyz","password":"xyz"}' localhost:5000/bolt/users/user1
curl -X PUT -H "Content-Type: text/html" -d 'some value' localhost:5000/bolt/users/user2

GET:

# params
host/database/backet/key

curl localhost:5000/bolt/images/durov
return: bytes
curl localhost:5000/bolt/users/user1
return: {"username":"xyz","password":"xyz"}
curl -v localhost:5000/bolt/images/durov2
return 404 Error

POST:

# params
host/database/backet/key?cnt=1000&order=desc&vals=false

key: first key, possible values "some_your_key" or "some_your_key*" for prefix scan, Last, First - default Last
cnt: return count records, default 1000
order: sorting order (keys ordered as strings!), default desc
vals: return values, default false

curl -X POST localhost:5000/bolt/users
return: {"user2","user1"}

curl -X POST localhost:5000/bolt/users/First?cnt=2&order=asc
return: {"user1"}

curl -X POST "http://localhost:5000/bolt/users/use*?order=asc&vals=true"
return: {"user1":"{"username":"xyz","password":"xyz"}","user2":"some value"}

curl -X POST "http://localhost:5000/bolt/users/user2?order=desc&vals=true"
return: {"user2":"some value","user1":"{"username":"xyz","password":"xyz"}"}

DELETE:

curl -X DELETE http://localhost:5000/bolt/users/user2
return 200 Ok (or 404 Error if bucket! not found)
*/
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

var boltdb *bolt.DB

// main handler
// default path localhost:5000/bolt/
func handlerBolt(w http.ResponseWriter, r *http.Request) {
	if boltdb == nil {
		//open boltdb on first call
		var err error
		boltdb, err = bolt.Open("bolt.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			fmt.Printf("%s\n", err)
		}
	}
	parser(w, r)
}

func handlerSlowPoke(w http.ResponseWriter, r *http.Request) {
	parser(w, r)
}

// Serve run server
// example addr: ":5000"
// example usage ./simpleserver :5000>>simpleserver.log &
func Serve(addr string) {

	http.HandleFunc("/bolt/", handlerBolt)
	http.HandleFunc("/slowpoke/", handlerSlowPoke)
	go func() {
		sigchan := make(chan os.Signal, 10)
		signal.Notify(sigchan, os.Interrupt)
		<-sigchan
		if boltdb != nil {
			boltdb.Close()
		}
		os.Exit(0)
	}()
	http.ListenAndServe(addr, nil)
}

func main() {
	argsWithProg := os.Args
	if len(argsWithProg) > 1 {
		Serve(os.Args[1])
	} else {
		Serve(":5000")
	}
}

// BoltAPI contains handler for rest api to boltdb
func parser(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	method := r.Method
	urlPart := strings.Split(r.URL.Path, "/")
	var err error
	var database = ""
	var bucketstr = ""
	var keystr = ""
	//log.Println("len", len(urlPart))
	if len(urlPart) == 4 {
		database = urlPart[1]
		bucketstr = urlPart[2]
		keystr = urlPart[3]
	}
	if len(urlPart) == 3 {
		database = urlPart[1]
		bucketstr = urlPart[2]
	}
	//pocessor(w, r, database, method, bucketstr, keystr)
	switch method {
	case "GET":
		if database == "bolt" && bucketstr == "backup" {
			boltdb.View(func(tx *bolt.Tx) error {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Disposition", `attachment; filename="my.db"`)
				w.Header().Set("Content-Length", strconv.FormatInt(int64(tx.Size()), 10))
				_, err := tx.WriteTo(w)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return nil
			})
		}
		val := get(database, bucketstr, keystr)
		if len(val) == 0 {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(val)
		}
		return
	case "PUT":

		v, err := ioutil.ReadAll(r.Body)
		if err == nil {
			err = put(database, bucketstr, keystr, v)
		}
		if err != nil {
			http.Error(w, err.Error(), 204)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	case "DELETE":

		err = delete(database, bucketstr, keystr)
		if err != nil {
			http.Error(w, err.Error(), 204)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	case "POST":
		cnt := r.URL.Query().Get("cnt")
		var order = r.URL.Query().Get("order")
		var max = 100000
		var vals = r.URL.Query().Get("vals")
		m, e := strconv.Atoi(cnt)
		if e == nil {
			max = m
		}
		val, err := post(database, bucketstr, keystr, order, vals, max)
		if err != nil {
			http.Error(w, err.Error(), 204)
			return
		}
		w.Write(val)
		return
	default:
		w.WriteHeader(404)
		return
	}
}

func post(database, bucketstr, keystr, order, vals string, max int) ([]byte, error) {
	var err error
	var buffer bytes.Buffer
	switch database {
	case "bolt":
		err = boltdb.View(func(tx *bolt.Tx) error {
			var prefix []byte
			b := tx.Bucket([]byte(bucketstr))
			if b == nil {
				return errors.New("NotFound")
			}
			c := b.Cursor()
			if keystr == "Last" || keystr == "" {
				k, _ := c.Last()
				keystr = string(k)
			}
			if keystr == "First" {
				k, _ := c.First()
				keystr = string(k)
			}
			if order == "" {
				order = "desc"
			}
			if vals == "" {
				vals = "false"
			}
			if strings.HasSuffix(keystr, "*") {
				prefix = []byte(keystr[:len(keystr)-1])
				keystr = keystr[:len(keystr)-1]
			}
			var comp = func(i int, m int, k []byte) bool {
				if prefix != nil {
					return i < m && bytes.HasPrefix(k, prefix)
				}
				return i < m
			}
			i := 0
			buffer.WriteString("[")
			switch order {
			case "asc":
				for k, v := c.Seek([]byte(keystr)); k != nil && comp(i, max, k); k, v = c.Next() {
					if i != 0 {
						buffer.WriteString(",")
					}
					if vals == "false" {
						buffer.WriteString(fmt.Sprintf("\"%s\"", k))
					} else {
						buffer.WriteString(fmt.Sprintf("{\"%s\":\"%s\"}", k, v))
					}
					i++
				}
			default:
				for k, v := c.Seek([]byte(keystr)); k != nil && comp(i, max, k); k, v = c.Prev() {
					if i != 0 {
						buffer.WriteString(",")
					}
					if vals == "false" {
						buffer.WriteString(fmt.Sprintf("\"%s\"", k))
					} else {
						buffer.WriteString(fmt.Sprintf("{\"%s\":\"%s\"}", k, v))
					}
					i++
				}

			}
			buffer.WriteString("]")

			return nil
		})

	}
	return buffer.Bytes(), err
}

func get(database, bucketstr, keystr string) []byte {
	var v []byte
	switch database {
	case "bolt":
		boltdb.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketstr))
			if b == nil {
				return nil
			}
			v = b.Get([]byte(keystr))
			return nil
		})
	}
	return v
}

func put(database, bucketstr, keystr string, val []byte) (err error) {
	switch database {
	case "bolt":
		err = boltdb.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucketstr))
			if err != nil {
				return err
			}
			e := b.Put([]byte(keystr), val)
			return e
		})
	}
	return err
}

func delete(database, bucketstr, keystr string) (err error) {
	switch database {
	case "bolt":
		return boltdb.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketstr))
			if b == nil {
				return errors.New("NotFound")
			}
			return b.Delete([]byte(keystr))
		})
	}
	return err
}
