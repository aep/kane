package kane

import (
	"net/url"
	"path/filepath"

	"github.com/aep/kane/kv"
)

type DB struct {
	kv.KV
}

func Init(connect ...string) (*DB, error) {
	if len(connect) < 1 {
		connect = append(connect, "tikv://localhost:2379")
	}

	uri, err := url.Parse(connect[0])
	if err != nil {
		return nil, err
	}

	var k kv.KV

	switch uri.Scheme {
	case "tikv":
		k, err = kv.NewTikv(uri.Host)
	case "pebble":
		path := filepath.Join(uri.Host, uri.Path)
		k, err = kv.NewPebble(path)
	default:
		k, err = kv.NewTikv(uri.Host)
	}

	if err != nil {
		return nil, err
	}

	return &DB{KV: k}, nil
}

func (db *DB) Close() {
	db.KV.Close()
}
