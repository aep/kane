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
}

func Eq(key string, val any) Filter {
	for _, ch := range key {
		if ch == 0xff {
			return Filter{err: fmt.Errorf("invalid key: cannot contain 0xff")}
		}
	}

	valb, err := indexVal(val)
	if err != nil {
		return Filter{err: err}
	}

	start := append([]byte{'.'}, key...)
	start = append(start, 0xff)
	start = append(start, valb...)
	start = append(start, 0xff)
	start = append(start, 0x00)
	end := bytes.Clone(start)
	end[len(end)-1] = 0xff

	return Filter{
		start: start,
		end:   end,
	}
}

func Has(key string) Filter {
	for _, ch := range key {
		if ch == 0xff {
			return Filter{err: fmt.Errorf("invalid key: cannot contain 0xff")}
		}
	}

	start := append([]byte{'.'}, key...)
	start = append(start, 0xff)
	start = append(start, 0x00)
	end := bytes.Clone(start)
	end[len(end)-1] = 0xff

	return Filter{
		start: start,
		end:   end,
	}
}

func (DB *DB) find(ctx context.Context, model string, op Filter) iter.Seq2[[]byte, error] {
	for _, ch := range model {
		if ch == 0xff {
			return func(yield func([]byte, error) bool) {
				yield(nil, fmt.Errorf("invalid key: cannot contain 0xff"))
			}
		}
	}

	start := append([]byte{'f', 0xff}, model...)
	start = append(start, 0xff)
	end := bytes.Clone(start)

	if op.err != nil {
		return func(yield func([]byte, error) bool) {
			yield(nil, op.err)
		}
	}
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
