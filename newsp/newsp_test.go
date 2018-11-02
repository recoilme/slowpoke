package main

import (
	"testing"
)

func TestMemBin(t *testing.T) {
	b := []byte("Hello")
	Set("", 1, b)
	var res []byte
	Get("", 1, &res)
	if string(res) != "Hello" {
		t.Error("Not bin", res, &res)
	}

	Set("", []byte("hello"), []byte("world!"))
	Get("", []byte("hello"), &res)
	if string(res) != "world!" {
		t.Error("Not bin", res, &res)
	}

	Set("", uint32(18), 427)
	var i int
	e := Get("", uint32(18), &i)

	if i != 427 {
		t.Error("Not 427", e)
	}
}

/*
func TestBase(t *testing.T) {
	b := []byte{0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40}
	type Tmp struct {
		F1 string
		F2 int8
		f3 int16
	}
	tmpw := &Tmp{F1: "s", F2: 3, f3: 5}
	var data = []interface{}{
		uint16(61374),
		int8(-54),
		uint8(254),
		true,
		"str",
		b,
		[]byte("45"),
		float32(1.2),
		tmpw,
	}
	for _, v := range data {
		e := Set("", v, v)
		if e != nil {
			t.Error(e)
		}
	}
	f := uint16(61374)
	e := Get("", f, &f)
	if e != nil {
		t.Error(e)
	}
	if f != uint16(61374) {
		t.Error("not eq")
	}

	bb := b
	var bbb []byte
	e = Get("", bb, &bbb)
	if e != nil {
		t.Error(e)
	}
	if bytes.Compare(bb, bbb) != 0 {
		t.Error("not eq")
	}
}
*/
