// Package slowpoke implements a low-level key/value store in pure Go.
// Keys stored in memory, Value stored on disk
// It uses locking for multiple readers and a single writer.
package slowpoke

import (
	"fmt"
)

func Log(i interface{}) {
	fmt.Println(i)
}
