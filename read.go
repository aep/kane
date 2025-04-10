package kane

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/aep/kane/kv"
)

func (DB *DB) Get(ctx context.Context, doc any, key string, op Filter) error {
	lifetime := &kv.Lifetime{}
	defer lifetime.Close()

	model := getModelFromAny(doc)

	for _, ch := range key {
		if ch == 0xff {
			return fmt.Errorf("invalid key: cannot contain 0xff")
		}
	}

	var id []byte
	for k, err := range DB.find(ctx, model, key, op) {
		if err != nil {
			return err
		}
		id = k
		break
	}
	if id == nil {
		return fmt.Errorf("not found")
	}

	path := append([]byte{'o', 0xff}, model...)
	path = append(path, 0xff)
	path = append(path, []byte(id)...)
	path = append(path, 0xff)

	b, err := DB.KV.Get(ctx, path)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(reflect.TypeOf(doc).String(), "*kane.Document") {
		doc = &Document{Val: doc}
	}

	err = deserializeStore(b, &doc)
	if err != nil {
		return err
	}
	return nil
}
