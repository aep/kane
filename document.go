package kane

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Document interface {
	PK() any
}

type StoredDocument struct {
	Val     any      `json:"val"`
	History *History `json:"history,omitempty"`
}

type History struct {
	Created *time.Time `json:"created,omitempty"`
	Updated *time.Time `json:"updated,omitempty"`
}

func getPKFromAny(val any) ([]byte, error) {
	if sdoc, ok := val.(*StoredDocument); ok {
		val = sdoc.Val
	}
	if doc, ok := val.(Document); ok {
		return indexVal(doc.PK())
	}

	return nil, fmt.Errorf("%T does not implement kane.Document: missing PK()", val)
}

func getModelFromAny(val any) string {
	if doc, ok := val.(StoredDocument); ok {
		return getModelFromAny(doc.Val)
	}

	if doc, ok := val.(*StoredDocument); ok {
		return getModelFromAny(doc.Val)
	}

	t := reflect.TypeOf(val)

	// Dereference pointers to get the underlying type
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Get the type name
	fullName := t.String()

	// Extract just the type name without the package for non-generic types
	if lastDot := strings.LastIndex(fullName, "."); lastDot >= 0 {
		return fullName[lastDot+1:]
	}

	return fullName
}
