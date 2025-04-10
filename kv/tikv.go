package kv

import (
	"context"
	"iter"
	"log/slog"
	"os"

	pingcaplog "github.com/pingcap/log"

	"github.com/lmittmann/tint"
	"github.com/tikv/client-go/txnkv/oracle"
	"github.com/tikv/client-go/v2/config"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/rawkv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var tracer trace.Tracer

func init() {
	l, p, _ := pingcaplog.InitLogger(&pingcaplog.Config{})

	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	config.OutputPaths = []string{"stderr"}
	config.ErrorOutputPaths = []string{"stderr"}
	l, _ = config.Build()

	pingcaplog.ReplaceGlobals(l, p)

	tracer = otel.Tracer("github.com/aep/apogy/kv")
}

var log = slog.New(tint.NewHandler(os.Stderr, nil))

type Tikv struct {
	k *rawkv.Client
}

func (k *Tikv) Set(ctx context.Context, key []byte, value []byte, opts ...Opt) error {
	return k.k.Put(ctx, key, value)
}

func (k *Tikv) Get(ctx context.Context, key []byte, opts ...Opt) ([]byte, error) {
	v, err := k.k.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

func (k *Tikv) Del(ctx context.Context, key []byte, opts ...Opt) error {
	return k.k.Delete(ctx, key)
}

func (k *Tikv) Scan(ctx context.Context, start []byte, end []byte, max int, opts ...Opt) ([]KeyAndValue, error) {
	keys, values, err := k.k.Scan(ctx, start, end, max)
	if err != nil {
		return nil, err
	}

	r := make([]KeyAndValue, len(keys))
	for i := 0; i < len(keys); i++ {
		r[i] = KeyAndValue{K: keys[i], V: values[i]}
	}

	return r, nil
}

func (k *Tikv) Iter(ctx context.Context, start []byte, end []byte, opts ...Opt) iter.Seq2[KeyAndValue, error] {
	return func(yield func(KeyAndValue, error) bool) {
		_, span := tracer.Start(ctx, "kv.TikvWrite.Iter")
		defer span.End()

		const batchSize = 100

		currentKey := start
		for {
			// Scan from current position to end, with limit
			keys, values, err := k.k.Scan(ctx, currentKey, end, batchSize)
			if err != nil {
				log.Debug("[tikv].Iter: scan error:", "start", string(currentKey), "end", string(end), "err", err)
				if !yield(KeyAndValue{}, err) {
					return
				}
				break
			}

			if len(keys) == 0 {
				break
			}

			// Process the batch
			for i, key := range keys {
				log.Debug("[tikv].Iter:", "start", string(start), "end", string(end), "at", string(key))
				if !yield(KeyAndValue{K: key, V: values[i]}, nil) {
					return
				}
			}

			currentKey = keys[len(keys)-1]
			if currentKey[len(currentKey)-1] == 0xff {
				currentKey = append(currentKey, 0x00)
			} else {
				currentKey[len(currentKey)-1] += 1
			}
		}
	}
}

func (k *Tikv) IterKeys(ctx context.Context, start []byte, end []byte, opts ...Opt) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		_, span := tracer.Start(ctx, "kv.TikvWrite.Iter")
		defer span.End()

		const batchSize = 100

		currentKey := start
		for {

			keys, _, err := k.k.Scan(ctx, currentKey, end, batchSize, rawkv.ScanKeyOnly())
			if err != nil {
				if !yield(nil, err) {
					return
				}
				break
			}

			if len(keys) == 0 {
				break
			}

			for _, key := range keys {
				if !yield(key, nil) {
					return
				}
			}

			currentKey = keys[len(keys)-1]
			if currentKey[len(currentKey)-1] == 0xff {
				currentKey = append(currentKey, 0x00)
			} else {
				currentKey[len(currentKey)-1] += 1
			}

		}
	}
}

func (k *Tikv) Close() error {
	return k.k.Close()
}

func (k *Tikv) BatchGet(ctx context.Context, keys [][]byte, opts ...Opt) ([][]byte, error) {
	v, err := k.k.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (k *Tikv) CAS(ctx context.Context, key, previousValue, newValue []byte, opts ...Opt) ([]byte, bool, error) {
	return k.k.CompareAndSwap(ctx, key, previousValue, newValue)
}

func (k *Tikv) Ping(ctx context.Context) error {
	_, _, err := k.k.GetPDClient().GetTS(ctx)
	return err
}

func (k *Tikv) GetVectorTime(ctx context.Context) (uint64, error) {
	p, l, err := k.k.GetPDClient().GetTS(ctx)
	if err != nil {
		return 0, err
	}
	return oracle.ComposeTS(p, l), nil
}

func NewTikv(ep string) (KV, error) {
	k, err := rawkv.NewClient(context.Background(), []string{ep}, config.DefaultConfig().Security)
	if err != nil {
		return nil, err
	}

	k.SetAtomicForCAS(true)

	return &Tikv{k}, nil
}
