package kane

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Document struct {
	// this is a monotonic global vector clock for serialization (tikv oracle), not wall time
	VTS uint64 `json:"vts"`

	Val     any      `json:"val"`
	History *History `json:"history,omitempty"`
}

type History struct {
	Created *time.Time `json:"created,omitempty"`
	Updated *time.Time `json:"updated,omitempty"`
}

func getIDFromAny(val any) ([]byte, error) {
	// Use reflection to check for ID field
	v := reflect.ValueOf(val)

	// Handle pointer types by dereferencing
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, fmt.Errorf("nil cannot be stored")
		}
		v = v.Elem()
	}

	// Only structs can have fields
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%T cannot be stored: must be struct", val)
	}

	// Look for ID field
	idField := v.FieldByName("ID")
	if !idField.IsValid() {
		return nil, fmt.Errorf("%T cannot be stored: must have field with name ID", val)
	}

	b, err := indexVal(idField.Interface())
	if err != nil {
		return nil, fmt.Errorf("%T cannot be stored: %w", val, err)
	}

	if len(b) < 3 {
		return nil, fmt.Errorf("%T cannot be stored: ID can't be empty", val)
	}

	return b, nil
}

func getModelFromAny(val any) string {
	if doc, ok := val.(Document); ok {
		return getModelFromAny(doc.Val)
	}

	if doc, ok := val.(*Document); ok {
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
