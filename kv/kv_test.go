package kv

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPebbleKV(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pebble-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "db")
	store, err := NewPebble(dbPath)
	if err != nil {
		t.Fatalf("Failed to create PebbleKV: %v", err)
	}
	defer store.Close()

	runKVTests(t, store)
}

func TestTiKV(t *testing.T) {
	// This requires a running TiKV instance
	store, err := NewTikv("127.0.0.1:2379")
	if err != nil {
		t.Skipf("Failed to connect to TiKV, skipping test: %v", err)
		return
	}
	defer store.Close()

	runKVTests(t, store)
}

// runKVTests runs a standard set of tests against any KV implementation
func runKVTests(t *testing.T, store KV) {
	t.Helper()
	ctx := context.Background()

	t.Run("Ping", func(t *testing.T) {
		if err := store.Ping(ctx); err != nil {
			t.Errorf("Ping failed: %v", err)
		}
	})

	t.Run("BasicOperations", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")

		// Test Set
		if err := store.Set(ctx, key, value); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Test Get
		retrievedValue, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(retrievedValue, value) {
			t.Errorf("Get returned wrong value: got %q, want %q", retrievedValue, value)
		}

		// Test Del
		if err := store.Del(ctx, key); err != nil {
			t.Fatalf("Del failed: %v", err)
		}

		// Verify key is gone
		_, err = store.Get(ctx, key)
		if err == nil {
			t.Errorf("Key should be deleted but still exists")
		}
	})

	t.Run("Lifetime", func(t *testing.T) {
		key := []byte("lifetime-key")
		value := []byte("lifetime-value")

		// Set the value
		if err := store.Set(ctx, key, value); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Get with lifetime
		var lifetime Lifetime
		retrievedValue, err := store.Get(ctx, key, &lifetime)
		if err != nil {
			t.Fatalf("Get with lifetime failed: %v", err)
		}
		if !bytes.Equal(retrievedValue, value) {
			t.Errorf("Get returned wrong value: got %q, want %q", retrievedValue, value)
		}

		// Close lifetime
		if err := lifetime.Close(); err != nil {
			t.Errorf("Failed to close lifetime: %v", err)
		}
	})

	t.Run("BatchGet", func(t *testing.T) {
		keys := [][]byte{
			[]byte("batch-key1"),
			[]byte("batch-key2"),
			[]byte("batch-key3"),
		}
		values := [][]byte{
			[]byte("batch-value1"),
			[]byte("batch-value2"),
			[]byte("batch-value3"),
		}

		// Set the values
		for i, key := range keys {
			if err := store.Set(ctx, key, values[i]); err != nil {
				t.Fatalf("Set failed for key %q: %v", key, err)
			}
		}

		// Batch get
		retrievedValues, err := store.BatchGet(ctx, keys)
		if err != nil {
			t.Fatalf("BatchGet failed: %v", err)
		}

		if len(retrievedValues) != len(values) {
			t.Fatalf("BatchGet returned wrong number of values: got %d, want %d", len(retrievedValues), len(values))
		}

		for i, val := range retrievedValues {
			if !bytes.Equal(val, values[i]) {
				t.Errorf("BatchGet returned wrong value at index %d: got %q, want %q", i, val, values[i])
			}
		}

		// Clean up
		for _, key := range keys {
			if err := store.Del(ctx, key); err != nil {
				t.Fatalf("Del failed: %v", err)
			}
		}
	})

	t.Run("CAS", func(t *testing.T) {
		key := []byte("cas-key")
		value1 := []byte("cas-value1")
		value2 := []byte("cas-value2")

		// Set initial value
		if err := store.Set(ctx, key, value1); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Successful CAS
		_, success, err := store.CAS(ctx, key, value1, value2)
		if err != nil {
			t.Fatalf("CAS failed: %v", err)
		}
		if !success {
			t.Errorf("CAS should have succeeded")
		}

		// Verify value changed
		retrievedValue, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(retrievedValue, value2) {
			t.Errorf("After CAS, got %q, want %q", retrievedValue, value2)
		}

		// Failed CAS with wrong previous value
		wrongPrevious := []byte("wrong-value")
		_, success, err = store.CAS(ctx, key, wrongPrevious, value1)
		if err != nil {
			t.Fatalf("CAS failed: %v", err)
		}
		if success {
			t.Errorf("CAS should have failed with incorrect previous value")
		}

		// Clean up
		if err := store.Del(ctx, key); err != nil {
			t.Fatalf("Del failed: %v", err)
		}
	})

	t.Run("ParallelCAS", func(t *testing.T) {
		key := []byte("parallel-cas-key")
		initialValue := []byte("initial-value")

		// Set initial value
		if err := store.Set(ctx, key, initialValue); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Number of goroutines and iterations
		numGoroutines := 10
		iterations := 20

		// Use a channel to track successful CAS operations
		successful := make(chan bool, numGoroutines*iterations)

		// Use a WaitGroup to wait for all goroutines to finish
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Launch goroutines
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()

				for j := 0; j < iterations; j++ {
					// Attempt to read current value
					current, err := store.Get(ctx, key)
					if err != nil {
						t.Logf("Goroutine %d: Get failed: %v", id, err)
						continue
					}

					// Create new value (concatenate the old value with our ID)
					newValue := append(bytes.Clone(current), byte(id), byte(j))

					// Attempt CAS operation
					_, success, err := store.CAS(ctx, key, current, newValue)
					if err != nil {
						t.Logf("Goroutine %d: CAS failed with error: %v", id, err)
						continue
					}

					// Record success or failure
					successful <- success
				}
			}(i)
		}

		// Wait for all goroutines to finish
		wg.Wait()
		close(successful)

		// Count successes
		var successCount int
		for success := range successful {
			if success {
				successCount++
			}
		}

		// Check that we had a reasonable number of successes
		// In a perfect world with no contention, all attempts would succeed
		// In reality, many will fail due to concurrent modifications
		if successCount == 0 {
			t.Errorf("Expected some CAS operations to succeed, but none did")
		}

		t.Logf("Parallel CAS: %d/%d operations succeeded", successCount, numGoroutines*iterations)

		// Verify final value is different from initial
		finalValue, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Final Get failed: %v", err)
		}

		if bytes.Equal(finalValue, initialValue) {
			t.Errorf("Final value should be different from initial value")
		}

		// Clean up
		if err := store.Del(ctx, key); err != nil {
			t.Fatalf("Del failed: %v", err)
		}
	})

	t.Run("Iter", func(t *testing.T) {
		// Prepare data
		keyValues := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("iter-key1"), []byte("iter-value1")},
			{[]byte("iter-key2"), []byte("iter-value2")},
			{[]byte("iter-key3"), []byte("iter-value3")},
			{[]byte("iter-key4"), []byte("iter-value4")},
		}

		for _, kv := range keyValues {
			if err := store.Set(ctx, kv.key, kv.value); err != nil {
				t.Fatalf("Set failed for key %q: %v", kv.key, err)
			}
		}

		// Test iterator
		start := []byte("iter-key1")
		end := []byte("iter-key5") // End is exclusive, so this should include all keys

		count := 0
		iter := store.Iter(ctx, start, end)
		iter(func(kv KeyAndValue, err error) bool {
			if err != nil {
				t.Errorf("Iter returned error: %v", err)
				return false
			}

			found := false
			for _, expectedKV := range keyValues {
				if bytes.Equal(kv.K, expectedKV.key) && bytes.Equal(kv.V, expectedKV.value) {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Unexpected key-value pair: %q -> %q", kv.K, kv.V)
			}

			count++
			return true
		})

		if count != len(keyValues) {
			t.Errorf("Iter returned wrong number of items: got %d, want %d", count, len(keyValues))
		}

		// Test key iterator
		keyCount := 0
		keyIter := store.IterKeys(ctx, start, end)
		keyIter(func(key []byte, err error) bool {
			if err != nil {
				t.Errorf("IterKeys returned error: %v", err)
				return false
			}

			found := false
			for _, expectedKV := range keyValues {
				if bytes.Equal(key, expectedKV.key) {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Unexpected key: %q", key)
			}

			keyCount++
			return true
		})

		if keyCount != len(keyValues) {
			t.Errorf("IterKeys returned wrong number of items: got %d, want %d", keyCount, len(keyValues))
		}

		// Clean up
		for _, kv := range keyValues {
			if err := store.Del(ctx, kv.key); err != nil {
				t.Fatalf("Del failed: %v", err)
			}
		}
	})

	t.Run("GetVectorTime", func(t *testing.T) {
		_, err := store.GetVectorTime(ctx)
		if err != nil {
			t.Logf("GetVectorTime returned error (expected for some implementations): %v", err)
		}
	})
}
