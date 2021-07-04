package block

import (
	"encoding/binary"
	"fmt"
)

func binarySizePanic(v interface{}) int {
	size := binary.Size(v)
	if size < 0 {
		panic(fmt.Sprintf("binary.Size(%T) = %d", v, size))
	}

	return size
}
