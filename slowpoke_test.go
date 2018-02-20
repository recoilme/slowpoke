package main

import (
	"bytes"
	"fmt"
	"testing"
)

type Vector struct {
	x, y, z int
}

func (v Vector) MarshalBinary() ([]byte, error) {
	// A simple encoding: plain text.
	var b bytes.Buffer
	fmt.Fprintln(&b, v.x, v.y, v.z)

	return b.Bytes(), nil
}

// UnmarshalBinary modifies the receiver so it must take a pointer receiver.
func (v *Vector) UnmarshalBinary(data []byte) error {
	// A simple encoding: plain text.
	b := bytes.NewBuffer(data)
	_, err := fmt.Fscanln(b, &v.x, &v.y, &v.z)
	return err
}

func TestT(t *testing.T) {

	//var network bytes.Buffer // Stand-in for the network.

	// Create an encoder and send a value.

	v := Vector{3, 4, 5}
	b, _ := v.MarshalBinary()
	vv := Vector{}
	vv.UnmarshalBinary(b)
	fmt.Printf("%v+", vv)

}
