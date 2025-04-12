package kane

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/aep/kane/kv"
)

func (DB *DB) Get(ctx context.Context, doc any, op Filter) error {
	lifetime := &kv.Lifetime{}
	defer lifetime.Close()

	model := getModelFromAny(doc)

	var ots []byte
	for k, err := range DB.find(ctx, model, op) {
		if err != nil {
			return err
		}
		ots = k
		break
	}
	if ots == nil {
		return fmt.Errorf("not found")
	}

	path := append([]byte{'o', 0xff}, ots...)
	path = append(path, 0xff)

	b, err := DB.KV.Get(ctx, path)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(reflect.TypeOf(doc).String(), "*kane.StoredDocument") {
		doc = &StoredDocument{Val: doc}
	}

	err = deserializeStore(b, &doc)
	if err != nil {
		return err
	}
	return nil
}
