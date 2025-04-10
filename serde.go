package kane

import (
	"bytes"
	"encoding/json"
	"errors"
)

func deserializeStore(b []byte, doc any) error {
	if len(b) < 1 {
		return nil
	}
	if b[0] != 'j' {
		return errors.New("invalid encoding stored in database")
	}
	dec := json.NewDecoder(bytes.NewReader(b[1:]))
	dec.UseNumber()
	return dec.Decode(doc)
}

func serializeStore(doc any) ([]byte, error) {
	b, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return append([]byte{'j'}, b...), nil
}
