package kv

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// createPebbleKV creates a temporary PebbleKV instance for testing
func createPebbleKV(t *testing.T) (KV, func()) {
	t.Helper()
	tempDir := filepath.Join(os.TempDir(), "pebble-test-"+time.Now().Format("20060102150405"))

	kv, err := NewPebble(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleKV: %v", err)
	}

	cleanup := func() {
		kv.Close()
		os.RemoveAll(tempDir)
	}

	return kv, cleanup
}

// TestKVImplementations runs the same tests against all KV implementations
func TestKVImplementations(t *testing.T) {
	// Define the implementations to test
	implementations := []struct {
		name    string
		factory func(t *testing.T) (KV, func())
	}{
		{"Pebble", createPebbleKV},
		{"TiKV", func(t *testing.T) (KV, func()) {
			tikvEndpoint := os.Getenv("TIKV_ENDPOINT")
			if tikvEndpoint == "" {
				tikvEndpoint = "127.0.0.1:2379" // Default PD endpoint
			}
			kv, err := NewTikv(tikvEndpoint)
			if err != nil {
				t.Fatalf("Failed to create TiKV client: %v", err)
			}
			return kv, func() { kv.Close() }
		}},
	}

	for _, impl := range implementations {
		t.Run(impl.name, func(t *testing.T) {
			kv, cleanup := impl.factory(t)
			defer cleanup()

			// Run all tests for this implementation
			t.Run("PingTest", func(t *testing.T) { testPing(t, kv) })
			t.Run("BasicOperations", func(t *testing.T) { testBasicOperations(t, kv) })
			t.Run("BatchOperations", func(t *testing.T) { testBatchOperations(t, kv) })
			t.Run("IteratorOperations", func(t *testing.T) { testIteratorOperations(t, kv) })
		})
	}
}

// testPing tests the Ping functionality
func testPing(t *testing.T, kv KV) {
	t.Helper()
	ctx := context.Background()

	err := kv.Ping(ctx)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

// testBasicOperations tests the basic Put, Get, Del operations
func testBasicOperations(t *testing.T, kv KV) {
	t.Helper()
	ctx := context.Background()

	// Test data
	key := []byte("test-key")
	value := []byte("test-value")

	// Test Put
	err := kv.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	retrievedValue, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(retrievedValue, value) {
		t.Errorf("Get returned incorrect value: got %q, want %q", retrievedValue, value)
	}

	// Test Del
	err = kv.Del(ctx, key)
	if err != nil {
		t.Fatalf("Del failed: %v", err)
	}

	// Verify key was deleted
	retrievedValue, err = kv.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get on deleted key failed: %v", err)
	}
	if len(retrievedValue) != 0 {
		t.Errorf("Key not properly deleted, value still exists: %q", retrievedValue)
	}
}

// testBatchOperations tests BatchGet functionality
func testBatchOperations(t *testing.T, kv KV) {
	t.Helper()
	ctx := context.Background()

	// Test data
	keys := [][]byte{
		[]byte("batch-key-1"),
		[]byte("batch-key-2"),
		[]byte("batch-key-3"),
	}
	values := [][]byte{
		[]byte("batch-value-1"),
		[]byte("batch-value-2"),
		[]byte("batch-value-3"),
	}

	// Put values individually
	for i := range keys {
		err := kv.Put(ctx, keys[i], values[i])
		if err != nil {
			t.Fatalf("Put failed for key %q: %v", keys[i], err)
		}
	}

	// Test BatchGet
	retrievedValues, err := kv.BatchGet(ctx, keys)
	if err != nil {
		t.Fatalf("BatchGet failed: %v", err)
	}

	foundValues := make(map[string]bool)
	for _, v := range retrievedValues {
		for _, expectedVal := range values {
			if bytes.Equal(v, expectedVal) {
				foundValues[string(expectedVal)] = true
			}
		}
	}

	for _, expectedVal := range values {
		if !foundValues[string(expectedVal)] {
			t.Errorf("BatchGet failed to retrieve expected value: %q", expectedVal)
		}
	}

	// Clean up
	for _, key := range keys {
		kv.Del(ctx, key)
	}
}

// testIteratorOperations tests the Iter functionality
func testIteratorOperations(t *testing.T, kv KV) {
	t.Helper()
	ctx := context.Background()

	// Test data - we'll create keys with a common prefix for easier range queries
	prefix := []byte("iter-test-")
	kvPairs := []struct {
		key   []byte
		value []byte
	}{
		{append(prefix, []byte("a")...), []byte("value-a")},
		{append(prefix, []byte("b")...), []byte("value-b")},
		{append(prefix, []byte("c")...), []byte("value-c")},
		{append(prefix, []byte("d")...), []byte("value-d")},
		{append(prefix, []byte("e")...), []byte("value-e")},
	}

	// Insert the data
	for _, pair := range kvPairs {
		err := kv.Put(ctx, pair.key, pair.value)
		if err != nil {
			t.Fatalf("Put failed for key %q: %v", pair.key, err)
		}
	}

	// Test Iter over a range (a-d, exclusive of e)
	start := append(prefix, []byte("a")...)
	end := append(prefix, []byte("e")...) // This is exclusive in the range

	var results []KeyAndValue
	iterFn := kv.Iter(ctx, start, end)
	iterFn(func(item KeyAndValue, err error) bool {
		if err != nil {
			t.Fatalf("Iterator returned error: %v", err)
			return false
		}
		results = append(results, item)
		return true
	})

	// Verify we got the expected number of results
	expectedCount := 4 // a, b, c, d but not e
	if len(results) != expectedCount {
		t.Errorf("Iterator returned wrong number of items: got %d, want %d", len(results), expectedCount)
	}

	// Verify the results are correct
	foundPairs := make(map[string]bool)
	for _, result := range results {
		for _, expected := range kvPairs[:4] { // Only check a-d
			if bytes.Equal(result.K, expected.key) && bytes.Equal(result.V, expected.value) {
				foundPairs[string(expected.key)] = true
			}
		}
	}

	for _, expected := range kvPairs[:4] { // Only check a-d
		if !foundPairs[string(expected.key)] {
			t.Errorf("Iterator failed to return expected key-value pair: %q -> %q",
				expected.key, expected.value)
		}
	}

	// Clean up
	for _, pair := range kvPairs {
		kv.Del(ctx, pair.key)
	}
}

// TestLifetime tests the Lifetime feature for memory management
func TestLifetime(t *testing.T) {
	kv, cleanup := createPebbleKV(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("lifetime-test-key")
	value := []byte("lifetime-test-value")

	// Put a value
	err := kv.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Create a lifetime and use it with Get
	lifetime := &Lifetime{}

	// Get with lifetime
	retrievedValue, err := kv.Get(ctx, key, lifetime)
	if err != nil {
		t.Fatalf("Get with lifetime failed: %v", err)
	}

	if !bytes.Equal(retrievedValue, value) {
		t.Errorf("Get returned incorrect value: got %q, want %q", retrievedValue, value)
	}

	// Close the lifetime
	err = lifetime.Close()
	if err != nil {
		t.Errorf("Lifetime.Close failed: %v", err)
	}

	// Delete the key
	kv.Del(ctx, key)
}
