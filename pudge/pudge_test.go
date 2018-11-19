package pudge

import (
	"fmt"
	"strconv"
	"testing"
)

const (
	f = "test/1"
)

func TestOpen(t *testing.T) {
	_, err := Open("", nil)
	if err == nil {
		t.Error("Open empty must error")
	}
	db, err := Open(f, &Config{FileMode: 0777, DirMode: 0777})
	if err != nil {
		t.Error(err)
	}
	err = db.DeleteFile()
	if err != nil {
		t.Error(err)
	}
}

func TestSet(t *testing.T) {
	db, err := Open(f, nil)
	if err != nil {
		t.Error(err)
	}
	err = db.Set(1, 1)
	if err != nil {
		t.Error(err)
	}
	err = db.DeleteFile()
	if err != nil {
		t.Error(err)
	}
}

func TestGet(t *testing.T) {
	db, err := Open(f, nil)
	if err != nil {
		t.Error(err)
	}
	err = db.Set(1, 1)
	if err != nil {
		t.Error(err)
	}
	var val int
	err = db.Get(1, &val)
	if err != nil {
		t.Error(err)
		return
	}

	if val != 1 {
		t.Error("val != 1", val)
		return
	}
	db.Close()

	err = db.DeleteFile()
	if err != nil {
		t.Error(err)
	}
}

func TestKeys(t *testing.T) {

	f := "test/keys.db"
	DeleteFile(f)
	db, err := Open(f, nil)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	append := func(i int) {
		k := []byte(fmt.Sprintf("%02d", i))
		v := []byte("Val:" + strconv.Itoa(i))
		err := db.Set(k, v)
		if err != nil {
			t.Error(err)
		}
	}
	for i := 1; i <= 20; i++ {
		append(i)
	}

	//ascending
	res, err := db.Keys(nil, 0, 0, true)
	var s = ""
	for _, r := range res {
		s += string(r)
	}
	if s != "0102030405060708091011121314151617181920" {
		t.Error("not asc", s)
	}

	//descending
	resdesc, err := db.Keys(nil, 0, 0, false)
	s = ""
	for _, r := range resdesc {
		s += string(r)
	}
	if s != "2019181716151413121110090807060504030201" {
		t.Error("not desc", s)
	}

	//offset limit asc
	reslimit, err := db.Keys(nil, 2, 2, true)
	s = ""
	for _, r := range reslimit {
		s += string(r)
	}
	if s != "0304" {
		t.Error("not off", s)
	}

	//offset limit desc
	reslimitdesc, err := db.Keys(nil, 2, 2, false)
	s = ""
	for _, r := range reslimitdesc {
		s += string(r)
	}
	if s != "1817" {
		t.Error("not off desc", s)
	}
}
