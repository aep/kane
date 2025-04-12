package kane

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type TestDoc struct {
	ID   string
	Name string
}

func (t *TestDoc) PK() any {
	return t.ID
}

func TestReadOperations(t *testing.T) {
	runTests := func(t *testing.T, db *DB) {
		ctx := context.Background()

		t.Run("Get", func(t *testing.T) {
			// Create a document to retrieve
			doc := &TestDoc{ID: "read-test-1", Name: "Test Document"}
			err := db.Put(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to create test document: %v", err)
			}

			// Try to retrieve it
			retrievedDoc := &TestDoc{ID: "read-test-1"}
			err = db.Get(ctx, retrievedDoc, Eq("ID", "read-test-1"))
			if err != nil {
				t.Fatalf("Failed to retrieve document: %v", err)
			}

			// Verify contents
			if retrievedDoc.Name != "Test Document" {
				t.Errorf("Retrieved document has wrong data: got %q, want %q", retrievedDoc.Name, "Test Document")
			}

			// Test not found case
			notFoundDoc := &TestDoc{ID: "does-not-exist"}
			err = db.Get(ctx, notFoundDoc, Eq("ID", "does-not-exist"))
			if err == nil {
				t.Error("Expected error for non-existent document, got nil")
			}

			// Clean up
			err = db.Del(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to delete test document: %v", err)
			}
		})
	}

	// Run tests with Pebble
	t.Run("Pebble", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "pebble-read-test")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)

		dbPath := filepath.Join(tempDir, "db")
		db, err := Init("pebble://" + dbPath)
		if err != nil {
			t.Fatalf("Failed to create PebbleDB: %v", err)
		}
		defer db.Close()

		runTests(t, db)
	})

	// Run tests with TiKV - skip if not available
	t.Run("TiKV", func(t *testing.T) {
		db, err := Init("tikv://127.0.0.1:2379")
		if err != nil {
			t.Fatalf("Failed to connect to TiKV, skipping test: %v", err)
		}
		defer db.Close()

		runTests(t, db)
	})
}

