package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aep/kane"
)

func main() {
	db, err := kane.Init()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for k, err := range db.IterKeys(context.Background(), []byte{0x00}, nil) {
		if err != nil {
			panic(err)
		}
		fmt.Println(escapeNonPrintable(k))
	}
}

func escapeNonPrintable(b []byte) string {
	var result strings.Builder
	for _, c := range b {
		if c >= 32 && c <= 126 {
			result.WriteByte(c)
		} else {
			result.WriteString(fmt.Sprintf("\\x%02x", c))
		}
	}
	return result.String()
}
