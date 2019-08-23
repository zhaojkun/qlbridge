package boltdb

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/schema"
)

func CreateDB(name string) (*Source, error) {
	db := New(name)
	if err := schema.RegisterSourceAsSchema(name, db); err != nil {
		u.Errorf("Could not read schema %v", err)
		return nil, err
	}
	return db, nil
}
