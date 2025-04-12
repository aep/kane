package kane

import (
	"context"
	"encoding/binary"
	"reflect"
	"strings"
	"time"
)

func (DB *DB) Del(ctx context.Context, old any) error {
	return DB.swap(ctx, nil, old, true)
}

// create an object. errors if an object with the same primary key exists
func (DB *DB) Put(ctx context.Context, doc any) error {
	return DB.swap(ctx, doc, nil, false)
}

// set an object. overwrites any existing object with the same primary key
func (DB *DB) Set(ctx context.Context, doc any) error {
	return DB.swap(ctx, doc, nil, true)
}

// set an object and delete and return any previous object with the same primary key if it existed
func (DB *DB) Swap(ctx context.Context, doc any, old any) error {
	return DB.swap(ctx, doc, old, true)
}

func (DB *DB) swap(ctx context.Context, doc any, old any, retry bool) error {
	var ots []byte
	var pk []byte
	var model string

	if doc == nil {
		var err error
		pk, err = getPKFromAny(old)
		if err != nil {
			return err
		}
		model = getModelFromAny(old)
	} else {

		var err error
		pk, err = getPKFromAny(doc)
		if err != nil {
			return err
		}
		model = getModelFromAny(doc)

		ots_, err := DB.KV.GetVectorTime(ctx)
		if err != nil {
			return err
		}
		var ots__ [8]byte
		binary.LittleEndian.PutUint64(ots__[:], ots_)
		ots = ots__[:]

		path := []byte{'o', 0xff, ots[0], ots[1], ots[2], ots[3], ots[4], ots[5], ots[6], ots[7], 0xff}

		if !strings.HasPrefix(reflect.TypeOf(doc).String(), "*kane.StoredDocument") {
			doc = &StoredDocument{Val: doc}
		}

		b, err := serializeStore(doc)
		if err != nil {
			return err
		}

		err = DB.KV.Set(ctx, path, b)
		if err != nil {
			return err
		}
		err = DB.index(ctx, doc.(*StoredDocument), []byte(model), ots[:], true)
		if err != nil {
			DB.KV.Del(ctx, path)
			return err
		}

		if pk == nil {
			return nil
		}

	}

	pkpath := append([]byte{'k', 0xff}, model...)
	pkpath = append(pkpath, 0xff)
	pkpath = append(pkpath, pk...)
	pkpath = append(pkpath, 0xff)

	var err error
	var oldots []byte

	for {
		var swapped bool
		oldots, swapped, err = DB.KV.CAS(ctx, pkpath, oldots, ots[:])
		if err != nil {

			if ots != nil {
				DB.index(ctx, doc.(*StoredDocument), []byte(model), ots[:], false)
				DB.KV.Del(ctx, []byte{'o', 0xff, ots[0], ots[1], ots[2], ots[3], ots[4], ots[5], ots[6], ots[7], 0xff})
			}
			return err
		}

		if swapped {
			if ots == nil {
				DB.KV.Del(ctx, pkpath)
			}
			break
		}

		select {
		case <-ctx.Done():
			if ots != nil {
				DB.index(ctx, doc.(*StoredDocument), []byte(model), ots[:], false)
				DB.KV.Del(ctx, []byte{'o', 0xff, ots[0], ots[1], ots[2], ots[3], ots[4], ots[5], ots[6], ots[7], 0xff})
			}
			return ctx.Err()
		default:
		}
		time.Sleep(time.Millisecond * 10)
		continue
	}

	if len(oldots) >= 8 {

		path := []byte{'o', 0xff, oldots[0], oldots[1], oldots[2], oldots[3], oldots[4], oldots[5], oldots[6], oldots[7], 0xff}
		oldb, err := DB.KV.Get(ctx, path)
		if err != nil {
			return nil
		}

		if old == nil {
			old = &StoredDocument{}
		}
		if !strings.HasPrefix(reflect.TypeOf(old).String(), "kane.StoredDocument") && !strings.HasPrefix(reflect.TypeOf(old).String(), "*kane.StoredDocument") {
			old = &StoredDocument{Val: old}
		}

		err = deserializeStore(oldb, old)
		if err == nil {
			DB.index(ctx, old.(*StoredDocument), []byte(model), oldots[:], false)
			DB.KV.Del(ctx, path)
		}
	}

	return nil
}
