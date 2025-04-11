package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	"github.com/aep/kane"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kane",
	Short: "CLI for Kane",
	Long:  `A CLI application for interacting with Kane databases.`,
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "dump raw kv for backup",
	Run: func(cmd *cobra.Command, args []string) {
		db, err := kane.Init()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing database: %v\n", err)
			os.Exit(1)
		}
		defer db.Close()

		o := os.Stdout

		o.Write([]byte("KANE1\n"))
		for kv, err := range db.KV.Iter(context.Background(), []byte{0x00}, nil) {
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error iterating keys: %v\n", err)
				os.Exit(1)
			}

			binary.Write(o, binary.BigEndian, uint64(len(kv.K)))
			o.Write(kv.K)
			binary.Write(o, binary.BigEndian, uint64(len(kv.V)))
			o.Write(kv.V)
		}
	},
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "restore database from backup",
	Run: func(cmd *cobra.Command, args []string) {
		db, err := kane.Init()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing database: %v\n", err)
			os.Exit(1)
		}
		defer db.Close()

		i := os.Stdin

		header := make([]byte, 6)
		if _, err := i.Read(header); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading backup header: %v\n", err)
			os.Exit(1)
		}

		if string(header) != "KANE1\n" {
			fmt.Fprintf(os.Stderr, "Invalid backup format\n")
			os.Exit(1)
		}

		count := 0

		ctx := context.Background()
		for {
			var keyLen uint64
			if err := binary.Read(i, binary.BigEndian, &keyLen); err != nil {
				if err.Error() == "EOF" {
					break
				}
				fmt.Fprintf(os.Stderr, "Error reading key length: %v\n", err)
				os.Exit(1)
			}

			key := make([]byte, keyLen)
			if _, err := i.Read(key); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading key: %v\n", err)
				os.Exit(1)
			}

			var valueLen uint64
			if err := binary.Read(i, binary.BigEndian, &valueLen); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading value length: %v\n", err)
				os.Exit(1)
			}

			value := make([]byte, valueLen)
			if _, err := i.Read(value); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading value: %v\n", err)
				os.Exit(1)
			}

			if err := db.KV.Set(ctx, key, value); err != nil {
				fmt.Fprintf(os.Stderr, "Error restoring key-value pair: %v\n", err)
				os.Exit(1)
			}

			count++
		}

		fmt.Printf("Restored %d key/values\n", count)
	},
}

var debugCmd = &cobra.Command{
	Use:   "debug",
	Short: "dump raw keys for debug",
	Run: func(cmd *cobra.Command, args []string) {
		db, err := kane.Init()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing database: %v\n", err)
			os.Exit(1)
		}
		defer db.Close()

		for k, err := range db.IterKeys(context.Background(), []byte{0x00}, nil) {
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error iterating keys: %v\n", err)
				os.Exit(1)
			}
			fmt.Println(escapeNonPrintable(k))
		}
	},
}

func escapeNonPrintable(b []byte) string {
	var result strings.Builder
	for _, c := range b {
		if c >= 32 && c <= 126 {
			result.WriteByte(c)
		} else {
			result.WriteString(fmt.Sprintf("\\x%02x", c))
		}
	}
	return result.String()
}

func init() {
	rootCmd.AddCommand(debugCmd)
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing command: %v\n", err)
		os.Exit(1)
	}
}
