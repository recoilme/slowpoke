package main

import "testing"

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
		_ = v
	}
}
