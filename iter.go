package kane

import (
	"context"
	"iter"
	"reflect"
)

func Iter[Val any](ctx context.Context, DB *DB, key string, op Filter) iter.Seq2[Val, error] {
	var val Val
	model := getModelFromAny(val)

	return func(yield func(Val, error) bool) {
		for id, err := range DB.find(ctx, model, key, op) {

			var rval Val
			if err != nil {
				if !yield(rval, err) {
					return
				}
			}

			path2 := append([]byte{'o', 0xff}, model...)
			path2 = append(path2, 0xff)
			path2 = append(path2, []byte(id)...)
			path2 = append(path2, 0xff)

			b, err := DB.KV.Get(ctx, path2)
			if err != nil {
				continue
			}

			var doc *Document
			if reflect.TypeOf(rval) == reflect.TypeOf(Document{}) {
				doc = any(&rval).(*Document)
			} else {
				doc = &Document{Val: &rval}
			}

			err = deserializeStore(b, doc)
			if err != nil {
				continue
			}

			if !yield(rval, nil) {
				return
			}
		}
	}
}
