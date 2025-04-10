package kane

import (
	"bytes"
	"context"
	"fmt"
	"iter"
)

type Filter struct {
	err error

	start []byte
	end   []byte

	optimizationIDEq []byte
}

func Eq(val any) Filter {
	start, err := indexVal(val)
	if err != nil {
		return Filter{err: err}
	}

	start = append(start, 0xff)
	start = append(start, 0x00)
	end := bytes.Clone(start)
	end[len(end)-1] = 0xff

	return Filter{
		start:            start,
		end:              end,
		optimizationIDEq: start[:len(start)-2],
	}
}

func (DB *DB) find(ctx context.Context, model string, key string, op Filter) iter.Seq2[[]byte, error] {
	if op.err != nil {
		return func(yield func([]byte, error) bool) {
			yield(nil, op.err)
		}
	}

	if key == "ID" && op.optimizationIDEq != nil {
		return func(yield func([]byte, error) bool) {
			yield(op.optimizationIDEq, nil)
		}
	}

	for _, ch := range key {
		if ch == 0xff {
			return func(yield func([]byte, error) bool) {
				yield(nil, fmt.Errorf("invalid key: cannot contain 0xff"))
			}
		}
	}

	for _, ch := range model {
		if ch == 0xff {
			return func(yield func([]byte, error) bool) {
				yield(nil, fmt.Errorf("invalid key: cannot contain 0xff"))
			}
		}
	}

	start := append([]byte{'f', 0xff}, model...)
	start = append(start, 0xff)
	start = append(start, []byte(key)...)
	start = append(start, 0xff)

	end := bytes.Clone(start)
	start = append(start, op.start...)
	end = append(end, op.end...)

	return func(yield func([]byte, error) bool) {
		for k, err := range DB.KV.IterKeys(ctx, start, end) {
			if err != nil {
				yield(nil, err)
				return
			}

			kk := bytes.Split(k, []byte{0xff})
			if len(kk) < 4 {
				continue
			}
			id := kk[len(kk)-2]

			if !yield(id, nil) {
				return
			}
		}
	}
}
