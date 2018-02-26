package slowpoke

import (
	"testing"
)

func ch(err error, t *testing.T) {
	if err != nil {
		t.Error(err)
	}
}

func TestSet(t *testing.T) {
	err := Set("1.db", []byte("1"), []byte("11"))
	if err != nil {
		t.Error(err)
	}
	err2 := Set("1.db", []byte("2"), []byte("22"))
	if err2 != nil {
		t.Error(err2)
	}
}

func TestRead(t *testing.T) {
	readTree("1.db")
}

func TestGet(t *testing.T) {
	res, err := Get("1.db", []byte("2"))
	if err != nil {
		t.Error(err)
	}
	log("Get:" + string(res))
}
