package kv

import (
	"context"
	"io"
	"iter"
)

type Opt any

type KeyAndValue struct {
	K []byte
	V []byte
}

type KV interface {
	Ping(ctx context.Context) error
	Close() error

	Get(ctx context.Context, key []byte, opts ...Opt) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...Opt) error
	Del(ctx context.Context, key []byte, opts ...Opt) error

	CAS(ctx context.Context, key, previousValue, newValue []byte, opts ...Opt) ([]byte, bool, error)

	BatchGet(ctx context.Context, keys [][]byte, opts ...Opt) ([][]byte, error)
	Iter(ctx context.Context, srart []byte, end []byte, opts ...Opt) iter.Seq2[KeyAndValue, error]
	IterKeys(ctx context.Context, srart []byte, end []byte, opts ...Opt) iter.Seq2[[]byte, error]

	GetVectorTime(ctx context.Context) (uint64, error)
}

// as an optimization, pass a pointer to a Lifetime, which must be Close()'d when done with the value
type Lifetime struct{ closers []io.Closer }

func (lf Lifetime) Close() error {
	var err error
	for _, closer := range lf.closers {
		err2 := closer.Close()
		if err == nil {
			err = err2
		}
	}
	return err
}
