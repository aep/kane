package kane

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// create an object. errors if an object with the same id and type exists
func (DB *DB) Put(ctx context.Context, doc any) error {
	id, err := getIDFromAny(doc)
	if err != nil {
		return err
	}

	model := getModelFromAny(doc)

	path := append([]byte{'o', 0xff}, model...)
	path = append(path, 0xff)
	path = append(path, id...)
	path = append(path, 0xff)

	if !strings.HasPrefix(reflect.TypeOf(doc).String(), "*kane.Document") {
		doc = &Document{Val: doc}
	}

	b, err := serializeStore(doc)
	if err != nil {
		return err
	}

	_, swapped, err := DB.KV.CAS(ctx, path, nil, b)
	if err != nil {
		return err
	}
	if !swapped {
		return fmt.Errorf("conflict: '%s' with id '%s' exists", model, id)
	}
	return nil
}

// set an object. overwrites any existing object with the same id and type
func (DB *DB) Set(ctx context.Context, doc any) error {
	return DB.Swap(ctx, doc, nil)
}

// set an object and return any previous object with the same id and type if it existed
func (DB *DB) Swap(ctx context.Context, doc any, old any) error {
	id, err := getIDFromAny(doc)
	if err != nil {
		return err
	}

	for _, ch := range id {
		if ch == 0xff {
			return fmt.Errorf("invalid model: cannot contain 0xff")
		}
	}

	if len(id) == 0 {
		return fmt.Errorf("invalid id: cannot be empty")
	}

	model := getModelFromAny(doc)

	path := append([]byte{'o', 0xff}, model...)
	path = append(path, 0xff)
	path = append(path, id...)
	path = append(path, 0xff)

	if !strings.HasPrefix(reflect.TypeOf(doc).String(), "*kane.Document") {
		doc = &Document{Val: doc}
	}

	doc.(*Document).VTS, err = DB.KV.GetVectorTime(ctx)
	if err != nil {
		return err
	}

	b, err := serializeStore(doc)
	if err != nil {
		return err
	}

	var oldb []byte
	for {
		var swapped bool
		oldb, swapped, err = DB.KV.CAS(ctx, path, oldb, b)
		if err != nil {
			return err
		}

		if swapped {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		time.Sleep(time.Millisecond * 10)
		continue
	}

	DB.index(ctx, doc.(*Document), []byte(model), id, true)

	if oldb != nil {

		// ok so we need to read the existing object to delete its indexes
		// howver there's a ABA problem where we might swap A->B and delete the index for A, but actually some other thread swapped it to A,
		// so we're deleting that index
		// to fix this, we're adding a timstamp to the index key, so that only really that index is deleted

		if old == nil {
			old = &Document{}
		}
		if !strings.HasPrefix(reflect.TypeOf(old).String(), "kane.Document") && !strings.HasPrefix(reflect.TypeOf(old).String(), "*kane.Document") {
			old = &Document{Val: old}
		}

		err := deserializeStore(oldb, old)
		if err == nil {
			DB.index(ctx, old.(*Document), []byte(model), id, false)
		}
	}

	return nil
}
