package boltdb

import (
	"errors"
	"reflect"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/iancoleman/strcase"
)

func CreateDB(name string) (*Source, error) {
	db := New(name)
	if err := schema.RegisterSourceAsSchema(name, db); err != nil {
		u.Errorf("Could not read schema %v", err)
		return nil, err
	}
	return db, nil
}

func struct2Fields(p interface{}) ([]Field, error) {
	rv := reflect.TypeOf(p)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, errors.New("not struct")
	}
	var rs []Field
	for i := 0; i < rv.NumField(); i++ {
		sf := rv.Field(i)
		if sf.Anonymous {
			continue
		}
		cf := Field{
			Name:    getFieldName(sf),
			ValType: getFieldType(sf),
			Size:    getFieldLen(sf),
		}
		rs = append(rs, cf)
	}
	return rs, nil
}

func getFieldName(sf reflect.StructField) string {
	dbTag := sf.Tag.Get("db")
	if dbTag != "" {
		return dbTag
	}
	gormTag := sf.Tag.Get("gorm")
	parts := strings.Split(gormTag, ";")
	for _, part := range parts {
		if strings.HasPrefix(part, "column:") {
			return strings.TrimPrefix(part, "column:")
		}
	}
	return strcase.ToSnake(sf.Name)
}

func getFieldType(sf reflect.StructField) value.ValueType {
	k := sf.Type.Kind()
	if k == reflect.Bool {
		return value.BoolType
	}
	if k >= reflect.Int && k <= reflect.Uint64 {
		return value.IntType
	}
	if k == reflect.String {
		return value.StringType
	}
	if sf.Type == reflect.TypeOf(time.Time{}) || sf.Type == reflect.TypeOf(&time.Time{}) {
		return value.TimeType
	}
	return value.NilType
}

func getFieldLen(sf reflect.StructField) int {
	return 64
}
