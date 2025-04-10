package kane

import (
	"github.com/aep/kane/kv"
	"net/url"
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

	k, err := kv.NewTikv(uri.Host)
	if err != nil {
		return nil, err
	}

	return &DB{KV: k}, nil
}

func (db *DB) Close() {
	db.KV.Close()
}
