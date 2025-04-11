package kv

import (
	"bytes"
	"context"
	"iter"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

type PebbleKV struct {
	db         *pebble.DB
	mu         sync.RWMutex  // Global RWLock to protect CAS
	vectorTime atomic.Uint64 // Vector time counter
}

func NewPebble(path string) (KV, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	pKV := &PebbleKV{db: db}

	// Initialize vector time from storage if it exists
	vtKey := []byte{'_', 0xff, 'v', 't', 's'}
	value, closer, err := db.Get(vtKey)
	if err == nil {
		if len(value) == 8 {
			currentValue := uint64(value[0]) |
				uint64(value[1])<<8 |
				uint64(value[2])<<16 |
				uint64(value[3])<<24 |
				uint64(value[4])<<32 |
				uint64(value[5])<<40 |
				uint64(value[6])<<48 |
				uint64(value[7])<<56
			pKV.vectorTime.Store(currentValue + 100)
		}
		closer.Close()
	} else if err != pebble.ErrNotFound {
		db.Close()
		return nil, err
	}

	return pKV, nil
}

func (p *PebbleKV) Ping(ctx context.Context) error {
	return nil
}

func (p *PebbleKV) Close() error {
	return p.db.Close()
}

func (p *PebbleKV) Get(ctx context.Context, key []byte, opts ...Opt) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	value, closer, err := p.db.Get(key)
	if err != nil {
		return nil, err
	}

	var lifetime *Lifetime
	for _, opt := range opts {
		if lf, ok := opt.(*Lifetime); ok {
			lifetime = lf
		}
	}

	if lifetime == nil {
		value = bytes.Clone(value)
		closer.Close()
	} else {
		lifetime.closers = append(lifetime.closers, closer)
	}

	return value, nil
}

// Put stores a key-value pair
func (p *PebbleKV) Set(ctx context.Context, key []byte, value []byte, opts ...Opt) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.db.Set(key, value, pebble.Sync)
}

// Del removes a key-value pair
func (p *PebbleKV) Del(ctx context.Context, key []byte, opts ...Opt) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.db.Delete(key, pebble.Sync)
}

// BatchGet retrieves multiple values for the given keys
func (p *PebbleKV) BatchGet(ctx context.Context, keys [][]byte, opts ...Opt) ([][]byte, error) {
	var r [][]byte

	for _, key := range keys {
		value, err := p.Get(ctx, key, opts)
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, err
		}
		r = append(r, value)
	}

	return r, nil
}

// Iter returns an iterator that yields key-value pairs in the range [start, end)
func (p *PebbleKV) Iter(ctx context.Context, start []byte, end []byte, opts ...Opt) iter.Seq2[KeyAndValue, error] {
	return func(yield func(KeyAndValue, error) bool) {
		p.mu.RLock()
		defer p.mu.RUnlock()

		iter, err := p.db.NewIter(&pebble.IterOptions{
			LowerBound: start,
			UpperBound: end,
		})
		if err != nil {
			yield(KeyAndValue{}, err)
			return
		}
		defer iter.Close()

		// Iterate through the range
		for iter.SeekGE(start); iter.Valid(); iter.Next() {
			// Make copies of key and value
			key := append([]byte{}, iter.Key()...)
			val := append([]byte{}, iter.Value()...)

			kv := KeyAndValue{
				K: key,
				V: val,
			}

			// Yield the key-value pair and stop if requested
			if !yield(kv, nil) {
				return
			}
		}

		// Check for errors after iteration
		if err := iter.Error(); err != nil {
			yield(KeyAndValue{}, err)
		}
	}
}

// Iter returns an iterator that yields key-value pairs in the range [start, end)
func (p *PebbleKV) IterKeys(ctx context.Context, start []byte, end []byte, opts ...Opt) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		p.mu.RLock()
		defer p.mu.RUnlock()

		iter, err := p.db.NewIter(&pebble.IterOptions{
			LowerBound: start,
			UpperBound: end,
		})
		if err != nil {
			yield(nil, err)
			return
		}
		defer iter.Close()

		// Iterate through the range
		for iter.SeekGE(start); iter.Valid(); iter.Next() {
			if !yield(bytes.Clone(iter.Key()), nil) {
				return
			}
		}

		// Check for errors after iteration
		if err := iter.Error(); err != nil {
			yield(nil, err)
		}
	}
}

func (p *PebbleKV) CAS(ctx context.Context, key, previousValue, newValue []byte, opts ...Opt) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	value, closer, err := p.db.Get(key)

	var currentValue []byte
	if err != nil {
		if err != pebble.ErrNotFound {
			return nil, false, err
		}
		// Handle not found case
		currentValue = nil
	} else {
		// We need to copy the value since we'll close the closer
		currentValue = bytes.Clone(value)
		closer.Close()
	}

	// If previousValue is nil, expect the key not to exist
	if previousValue == nil {
		if currentValue != nil {
			return currentValue, false, nil
		}
	} else {
		// Compare the current value with the expected previous value
		if !bytes.Equal(currentValue, previousValue) {
			return currentValue, false, nil
		}
	}

	// Values match, perform the swap
	err = p.db.Set(key, newValue, pebble.Sync)
	if err != nil {
		return nil, false, err
	}

	return nil, true, nil
}

func (p *PebbleKV) GetVectorTime(ctx context.Context) (uint64, error) {
	// Atomically increment and get the new value
	newValue := p.vectorTime.Add(1)

	if newValue%100 == 0 {
		go p.persistVectorTime(newValue)
	}

	return newValue, nil
}

// persistVectorTime writes the current vector time to disk
func (p *PebbleKV) persistVectorTime(value uint64) {
	vtKey := []byte{'_', 0xff, 'v', 't', 's'}

	valueBytes := []byte{
		byte(value),
		byte(value >> 8),
		byte(value >> 16),
		byte(value >> 24),
		byte(value >> 32),
		byte(value >> 40),
		byte(value >> 48),
		byte(value >> 56),
	}

	_ = p.db.Set(vtKey, valueBytes, nil)
}
