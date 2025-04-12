package kane

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

type IterTestDoc struct {
	ID   string
	Name string
	Age  int
}

func (d *IterTestDoc) PK() any {
	return d.ID
}

func TestIterOperations(t *testing.T) {
	runTests := func(t *testing.T, db *DB) {
		ctx := context.Background()

		// Prepare test data
		docs := []*IterTestDoc{
			{ID: "iter-test-1", Name: "Alice", Age: 25},
			{ID: "iter-test-2", Name: "Bob", Age: 30},
			{ID: "iter-test-3", Name: "Charlie", Age: 35},
			{ID: "iter-test-4", Name: "Dave", Age: 40},
			{ID: "iter-test-5", Name: "Eve", Age: 45},
		}

		// Insert test documents
		for _, doc := range docs {
			err := db.Put(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to put document %s: %v", doc.ID, err)
			}
		}

		t.Run("IterAllDocs", func(t *testing.T) {
			var retrievedDocs []IterTestDoc
			for doc, err := range Iter[IterTestDoc](ctx, db, Has("ID")) {
				if err != nil {
					t.Fatalf("Iteration error: %v", err)
				}
				retrievedDocs = append(retrievedDocs, doc)
			}

			// Since iteration order is not guaranteed, sort both slices for comparison
			sort.Slice(retrievedDocs, func(i, j int) bool {
				return retrievedDocs[i].ID < retrievedDocs[j].ID
			})

			// Verify all test docs were retrieved
			found := 0
			for _, expected := range docs {
				for _, actual := range retrievedDocs {
					if expected.ID == actual.ID {
						if expected.Name != actual.Name || expected.Age != actual.Age {
							t.Errorf("Retrieved document %s has wrong data: got {Name: %q, Age: %d}, want {Name: %q, Age: %d}",
								expected.ID, actual.Name, actual.Age, expected.Name, expected.Age)
						}
						found++
						break
					}
				}
			}

			if found < len(docs) {
				t.Errorf("Did not find all documents: got %d, want %d", found, len(docs))
			}
		})

		t.Run("IterWithFilter", func(t *testing.T) {
			// Iterate only over docs with Age >= 35
			var olderDocs []IterTestDoc
			for doc, err := range Iter[IterTestDoc](ctx, db, Eq("Age", 35)) {
				if err != nil {
					t.Fatalf("Iteration error: %v", err)
				}
				olderDocs = append(olderDocs, doc)
			}

			// There should be only 1 document with Age == 35
			if len(olderDocs) != 1 {
				t.Errorf("Expected 1 document with Age 35, got %d", len(olderDocs))
			}

			if len(olderDocs) > 0 && olderDocs[0].ID != "iter-test-3" {
				t.Errorf("Expected document with ID 'iter-test-3', got %s", olderDocs[0].ID)
			}
		})

		t.Run("IterWithNonExistentFilter", func(t *testing.T) {
			// Iterate with a filter that matches no documents
			count := 0
			for _, err := range Iter[IterTestDoc](ctx, db, Eq("Age", 999)) {
				if err != nil {
					t.Fatalf("Iteration error: %v", err)
				}
				count++
			}

			if count != 0 {
				t.Errorf("Expected 0 documents for non-existent filter, got %d", count)
			}
		})

		t.Run("IterWithBrokenFilter", func(t *testing.T) {
			// Test with a filter containing an invalid character
			success := true
			for _, err := range Iter[IterTestDoc](ctx, db, Eq("Invalid\xff", "value")) {
				if err == nil {
					success = false
				}
				break
			}

			if !success {
				t.Error("Expected error for invalid filter, but none was returned")
			}
		})

		// Clean up test documents
		for _, doc := range docs {
			err := db.Del(ctx, doc)
			if err != nil {
				t.Fatalf("Failed to delete document %s: %v", doc.ID, err)
			}
		}
	}

	// Run tests with Pebble
	t.Run("Pebble", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "pebble-iter-test")
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