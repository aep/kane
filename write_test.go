package kane

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type WriteTestDoc struct {
	ID    string
	Value string
}

func (d *WriteTestDoc) PK() any {
	return d.ID
}

func TestWriteOperations(t *testing.T) {
	runTests := func(t *testing.T, db *DB) {
		ctx := context.Background()

		t.Run("Put", func(t *testing.T) {
			// Create a new document
			doc := &WriteTestDoc{ID: "write-test-1", Value: "Original Value"}
			err := db.Put(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to put document: %v", err)
			}

			// Verify it was created
			retrievedDoc := &WriteTestDoc{ID: "write-test-1"}
			err = db.Get(ctx, retrievedDoc, Eq("ID", "write-test-1"))
			if err != nil {
				t.Fatalf("Failed to retrieve document after Put: %v", err)
			}

			if retrievedDoc.Value != "Original Value" {
				t.Errorf("Retrieved document has wrong data: got %q, want %q", retrievedDoc.Value, "Original Value")
			}

			// Attempt to create a document with same key
			duplicateDoc := &WriteTestDoc{ID: "write-test-1", Value: "Duplicate Value"}
			err = db.Put(ctx, duplicateDoc)
			if err == nil {
				t.Error("Expected error when putting document with existing key, got nil")
			}

			// Clean up
			err = db.Del(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to delete test document: %v", err)
			}
		})

		t.Run("Set", func(t *testing.T) {
			// Create a document
			doc := &WriteTestDoc{ID: "write-test-2", Value: "Initial Value"}
			err := db.Put(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to put document: %v", err)
			}

			// Update the document
			updatedDoc := &WriteTestDoc{ID: "write-test-2", Value: "Updated Value"}
			err = db.Set(ctx, updatedDoc)
			if err != nil {
				t.Fatalf("Failed to set document: %v", err)
			}

			// Verify update
			retrievedDoc := &WriteTestDoc{ID: "write-test-2"}
			err = db.Get(ctx, retrievedDoc, Eq("ID", "write-test-2"))
			if err != nil {
				t.Fatalf("Failed to retrieve document after Set: %v", err)
			}

			if retrievedDoc.Value != "Updated Value" {
				t.Errorf("Retrieved document has wrong data: got %q, want %q", retrievedDoc.Value, "Updated Value")
			}

			// Set a document that doesn't exist (should create it)
			newDoc := &WriteTestDoc{ID: "write-test-3", Value: "New Value"}
			err = db.Set(ctx, newDoc)
			if err != nil {
				t.Fatalf("Failed to set new document: %v", err)
			}

			// Verify creation
			retrievedNewDoc := &WriteTestDoc{ID: "write-test-3"}
			err = db.Get(ctx, retrievedNewDoc, Eq("ID", "write-test-3"))
			if err != nil {
				t.Fatalf("Failed to retrieve new document after Set: %v", err)
			}

			if retrievedNewDoc.Value != "New Value" {
				t.Errorf("Retrieved new document has wrong data: got %q, want %q", retrievedNewDoc.Value, "New Value")
			}

			// Clean up
			err = db.Del(ctx, updatedDoc)
			if err != nil {
				t.Fatalf("Failed to delete updated document: %v", err)
			}

			err = db.Del(ctx, newDoc)
			if err != nil {
				t.Fatalf("Failed to delete new document: %v", err)
			}
		})

		t.Run("Swap", func(t *testing.T) {
			// Create a document
			doc := &WriteTestDoc{ID: "write-test-4", Value: "Swap Original"}
			err := db.Put(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to put document: %v", err)
			}

			// Swap the document
			swapDoc := &WriteTestDoc{ID: "write-test-4", Value: "Swap New"}
			oldDoc := &WriteTestDoc{ID: "write-test-4"}
			err = db.Swap(ctx, swapDoc, oldDoc)
			if err != nil {
				t.Fatalf("Failed to swap document: %v", err)
			}

			// Verify the new document
			retrievedDoc := &WriteTestDoc{ID: "write-test-4"}
			err = db.Get(ctx, retrievedDoc, Eq("ID", "write-test-4"))
			if err != nil {
				t.Fatalf("Failed to retrieve document after Swap: %v", err)
			}

			if retrievedDoc.Value != "Swap New" {
				t.Errorf("Retrieved document has wrong data: got %q, want %q", retrievedDoc.Value, "Swap New")
			}

			// Verify the old document
			if oldDoc.Value != "Swap Original" {
				t.Errorf("Old document has wrong data: got %q, want %q", oldDoc.Value, "Swap Original")
			}

			// Clean up
			err = db.Del(ctx, swapDoc)
			if err != nil {
				t.Fatalf("Failed to delete test document: %v", err)
			}
		})

		t.Run("Del", func(t *testing.T) {
			// Create a document
			doc := &WriteTestDoc{ID: "write-test-5", Value: "Delete Me"}
			err := db.Put(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to put document: %v", err)
			}

			// Verify it was created
			retrievedDoc := &WriteTestDoc{ID: "write-test-5"}
			err = db.Get(ctx, retrievedDoc, Eq("ID", "write-test-5"))
			if err != nil {
				t.Fatalf("Failed to retrieve document after Put: %v", err)
			}

			// Delete the document
			err = db.Del(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to delete document: %v", err)
			}

			// Verify it was deleted
			err = db.Get(ctx, retrievedDoc, Eq("ID", "write-test-5"))
			if err == nil {
				t.Error("Expected error when getting deleted document, got nil")
			}

			// Delete a non-existent document
			nonExistentDoc := &WriteTestDoc{ID: "does-not-exist"}
			err = db.Del(ctx, nonExistentDoc)
			if err != nil {
				t.Fatalf("Failed when deleting non-existent document: %v", err)
			}
		})
	}

	// Run tests with Pebble
	t.Run("Pebble", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "pebble-write-test")
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
			t.Skipf("Failed to connect to TiKV, skipping test: %v", err)
			return
		}
		defer db.Close()

		runTests(t, db)
	})
}