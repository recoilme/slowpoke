package pudge

import (
	"testing"
	"time"
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
	time.Sleep(3 * time.Second)
	if val != 1 {
		t.Error("val != 1", val)
		return
	}
	db.Close()
	time.Sleep(5 * time.Second)
	err = db.DeleteFile()
	if err != nil {
		t.Error(err)
	}
}
