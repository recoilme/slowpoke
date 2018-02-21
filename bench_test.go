package slowpoke

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func BenchmarkSample(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if x := fmt.Sprintf("%d", 42); x != "42" {
			b.Fatalf("Unexpected string: %s", x)
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	InitDatabase()
	file := "benchwrite"
	for i := 0; i < b.N; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(i))
		Set(file, b, b)
	}
	CloseDatabase()
}
