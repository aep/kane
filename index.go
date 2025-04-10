package kane

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

const (
	ValueInvalid = iota
	ValueInteger
	ValueString
	ValueFloat
	ValueBytes
	ValueBool
)

func (DB *DB) index(ctx context.Context, doc *Document, model []byte, id []byte, creating bool) error {
	path := append([]byte{'f', 0xff}, model...)
	path = append(path, 0xff)

	var vts [8]byte
	binary.LittleEndian.PutUint64(vts[:], doc.VTS)
	postfix := append([]byte{0xff}, vts[:]...)
	postfix = append(postfix, 0xff)
	postfix = append(postfix, id...)
	postfix = append(postfix, 0xff)

	return DB.indexI(ctx, doc.Val, path, postfix, creating)
}

func (DB *DB) indexI(ctx context.Context, obj any, path []byte, postfix []byte, creating bool) error {
	if obj == nil {
		return nil
	}

	switch v := obj.(type) {
	case []interface{}:
		for _, v := range v {
			err := DB.indexI(ctx, v, path, postfix, creating)
			if err != nil {
				return err
			}
		}
	case *map[string]interface{}:
		if v == nil {
			return nil
		}
		for k, v := range *v {
			// make extra sure there is no 0xff anywhere in the data
			// it's not valid utf8 so this should not happen
			// if i dont check it, i'll probably make a mistake later that will allow a filter bypass

			kbin := []byte(k)
			safe := true
			for _, ch := range kbin {
				if ch == 0xff {
					safe = false
					break
				}
			}

			if safe {
				path2 := bytes.Clone(path)
				if path2[len(path)-1] != 0xff {
					path2 = append(path2, '.')
				}
				path2 = append(path2, kbin...)
				err := DB.indexI(ctx, v, path2, postfix, creating)
				if err != nil {
					return err
				}
			}
		}
	case map[string]interface{}:
		for k, v := range v {
			// make extra sure there is no 0xff anywhere in the data
			// it's not valid utf8 so this should not happen
			// if i dont check it, i'll probably make a mistake later that will allow a filter bypass

			kbin := []byte(k)
			safe := true
			for _, ch := range kbin {
				if ch == 0xff {
					safe = false
					break
				}
			}

			if safe {
				path2 := bytes.Clone(path)
				if path2[len(path)-1] != 0xff {
					path2 = append(path2, '.')
				}
				path2 = append(path2, kbin...)
				err := DB.indexI(ctx, v, path2, postfix, creating)
				if err != nil {
					return err
				}
			}
		}
	case []byte, string, json.Number, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		vbin, err := indexVal(v)
		if err != nil {
			return nil
		}
		pathW := append(bytes.Clone(path), 0xff)
		pathW = append(pathW, vbin...)
		pathW = append(pathW, postfix...)
		if creating {
			DB.KV.Set(ctx, pathW, []byte{0xff})
		} else {
			DB.KV.Del(ctx, pathW)
		}

	default:
		return DB.indexStruct(ctx, obj, path, postfix, creating)
	}

	return nil
}

func indexVal(val any) ([]byte, error) {
	switch v := val.(type) {
	case []byte:
		if len(v) > 1024 {
			return nil, fmt.Errorf("string too long for index")
		}
		for _, ch := range v {
			if ch == 0xff {
				return nil, fmt.Errorf("invalid string")
			}
		}

		vbin := append([]byte{ValueBytes}, v...)
		return vbin, nil

	case string:
		if len(v) > 1024 {
			return nil, fmt.Errorf("string too long for index")
		}
		for _, ch := range v {
			if ch == 0xff {
				return nil, fmt.Errorf("invalid string")
			}
		}

		vbin := append([]byte{ValueString}, []byte(v)...)
		return vbin, nil

	case json.Number:
		if i64, err := v.Int64(); err == nil {
			return indexVal(i64)
		} else if f64, err := v.Float64(); err == nil {
			return indexVal(f64)
		}
	case float32:
		vbin := make([]byte, 10)
		vbin[0] = ValueFloat
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case float64:
		vbin := make([]byte, 10)
		vbin[0] = ValueFloat
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case int:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		if v >= 0 {
			vbin[1] = 1
		}
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case int8:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		if v >= 0 {
			vbin[1] = 1
		}
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case int16:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		if v >= 0 {
			vbin[1] = 1
		}
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case int32:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		if v >= 0 {
			vbin[1] = 1
		}
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case int64:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		if v >= 0 {
			vbin[1] = 1
		}
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case uint:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		vbin[1] = 1
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case uint8:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		vbin[1] = 1
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case uint16:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		vbin[1] = 1
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case uint32:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		vbin[1] = 1
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case uint64:
		vbin := make([]byte, 10)
		vbin[0] = ValueInteger
		vbin[1] = 1
		binary.BigEndian.PutUint64(vbin[2:], uint64(v))
		return vbin, nil
	case bool:
		vbin := make([]byte, 2)
		vbin[0] = ValueBool
		if v {
			vbin[1] = 1
		}
		return vbin, nil
	}

	return nil, fmt.Errorf("%T cannot be used in index", val)
}

// indexStruct handles struct and struct pointer types similar to how json.Marshal would encode them
func (DB *DB) indexStruct(ctx context.Context, obj any, path []byte, postfix []byte, creating bool) error {
	v := reflect.ValueOf(obj)

	// Handle pointers by dereferencing them
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	// Only process if we have a struct
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("%T cannot be used in index", obj)
	}

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get the field value
		fieldValue := v.Field(i)

		// Skip nil pointer fields
		if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
			continue
		}

		// Process json tag to get field name
		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			// Skip fields explicitly marked to be excluded
			continue
		}

		fieldName := field.Name
		parts := strings.Split(jsonTag, ",")
		if len(parts) > 0 && parts[0] != "" {
			fieldName = parts[0]
		}

		// Check if fieldName contains 0xff
		kbin := []byte(fieldName)
		safe := true
		for _, ch := range kbin {
			if ch == 0xff {
				safe = false
				break
			}
		}

		if !safe {
			continue
		}

		path2 := bytes.Clone(path)
		if path2[len(path)-1] != 0xff {
			path2 = append(path2, '.')
		}
		path2 = append(path2, kbin...)

		// Get the field value and index it
		fieldInterface := fieldValue.Interface()
		err := DB.indexI(ctx, fieldInterface, path2, postfix, creating)
		if err != nil {
			return err
		}
	}

	return nil
}
