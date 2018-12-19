// Package slowpoke implements a low-level key/value store in pure Go.
// Keys stored in memory, Value stored on disk
package slowpoke

import (
	"bytes"
	"encoding/gob"
	"reflect"

	"github.com/recoilme/pudge"
)

// Set store val and key with sync at end
// File - may be existing file or new
// If path to file contains dirs - dirs will be created
// If val is nil - will store only key
func Set(file string, key []byte, val []byte) (err error) {
	return pudge.Set(file, key, val)
}

// Put store val and key with sync at end. It's wrapper for Set.
func Put(file string, key []byte, val []byte) (err error) {
	return pudge.Set(file, key, val)
}

// SetGob - experimental future for lazy usage, see tests
func SetGob(file string, key interface{}, val interface{}) (err error) {
	bufKey := bytes.Buffer{}
	if reflect.TypeOf(key).String() == "[]uint8" {
		v := key.([]byte)
		_, err = bufKey.Write(v)
	} else {
		err = gob.NewEncoder(&bufKey).Encode(key)
	}
	return pudge.Set(file, bufKey.Bytes(), val)
}

// Has return true if key exist or error if any
func Has(file string, key []byte) (exist bool, err error) {
	return pudge.Has(file, key)
}

// Count return count of keys or error if any
func Count(file string) (uint64, error) {
	cnt, err := pudge.Count(file)
	return uint64(cnt), err
}

// Counter return unique uint64
func Counter(file string, key []byte) (counter uint64, err error) {
	res, err := pudge.Counter(file, key, 1)
	return uint64(res), err
}

// Open open/create Db (with dirs)
// This operation is locked by mutex
// Return error if any
// Create .idx file for key storage
func Open(file string) (db *pudge.Db, err error) {
	return pudge.Open(file, nil)
}

// Get return value by key or nil and error
// Get will open Db if it closed
// return error if any
func Get(file string, key []byte) (val []byte, err error) {
	err = pudge.Get(file, key, &val)
	return val, err
}

// GetGob - experimental future for lazy usage, see tests
func GetGob(file string, key interface{}, val interface{}) (err error) {
	bufKey := bytes.Buffer{}
	if reflect.TypeOf(key).String() == "[]uint8" {
		v := key.([]byte)
		_, err = bufKey.Write(v)
	} else {
		err = gob.NewEncoder(&bufKey).Encode(key)
	}
	return pudge.Get(file, bufKey.Bytes(), val)
}

// Keys return keys in ascending  or descending order (false - descending,true - ascending)
// if limit == 0 return all keys
// if offset>0 - skip offset records
// If from not nil - return keys after from (from not included)
// If last byte of from == "*" - return keys with this prefix
func Keys(file string, from []byte, limit, offset uint32, asc bool) ([][]byte, error) {
	if from == nil {
		return pudge.Keys(file, nil, int(limit), int(offset), asc)
	}
	return pudge.Keys(file, from, int(limit), int(offset), asc)
}

// Close - close Db and free used memory
// It run finalizer and cancel goroutine
func Close(file string) (err error) {
	return pudge.Close(file)
}

// CloseAll - close all opened Db
func CloseAll() (err error) {
	return pudge.CloseAll()
}

// DeleteFile close file key and file val and delete db from map and disk
// All data will be loss!
func DeleteFile(file string) (err error) {
	return pudge.DeleteFile(file)
}

// Gets return key/value pairs in random order
// result contains key and value
// Gets not return error if key not found
// If no keys found return empty result
func Gets(file string, keys [][]byte) (result [][]byte) {
	db, err := Open(file)
	if err != nil {
		return nil
	}

	for _, key := range keys {
		var v []byte
		err := db.Get(key, &v)
		if err == nil {
			result = append(result, key)
			result = append(result, v)
		}
	}
	return result
}

// Sets store vals and keys
// Sync will called only at end of insertion
// Use it for mass insertion
// every pair must contain key and value
func Sets(file string, pairs [][]byte) (err error) {

	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	for i := range pairs {
		if i%2 != 0 {
			// on odd - append val and store key
			if pairs[i] == nil || pairs[i-1] == nil {
				break
			}
			err = db.Set(pairs[i-1], pairs[i])
			if err != nil {
				break
			}
		}
	}
	return err
}

// Delete key (always return true)
// Delete not remove any data from files
// Return error if any
func Delete(file string, key []byte) (bool, error) {
	err := pudge.Delete(file, key)
	if err == nil {
		return true, nil
	}
	return false, err
}
