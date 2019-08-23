package boltdb

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/boltdb/bolt"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	// Ensure this Csv Data Source implements expected interfaces
	_ schema.Source       = (*Source)(nil)
	_ schema.Alter        = (*Source)(nil)
	_ schema.Conn         = (*Table)(nil)
	_ schema.ConnUpsert   = (*Table)(nil)
	_ schema.ConnDeletion = (*Table)(nil)
)

// Source DataSource for testing creates an in memory b-tree per "table".
// Is not thread safe.
type Source struct {
	dbname        string
	db            *bolt.DB
	s             *schema.Schema
	tablenamelist []string
	tables        map[string]*StaticDataSource
	raw           map[string]string
}

// Table converts the static csv-source into a schema.Conn source
type Table struct {
	*StaticDataSource
}

// New create csv mock source.
func New(name string) *Source {
	dbpath := fmt.Sprintf("%s.db", name)
	os.Remove(dbpath)
	db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Panic(err)
	}
	return &Source{
		db:            db,
		dbname:        name,
		tablenamelist: make([]string, 0),
		raw:           make(map[string]string),
		tables:        make(map[string]*StaticDataSource),
	}
}

// Init no-op meets interface
func (m *Source) Init() {}

// Setup accept schema
func (m *Source) Setup(s *schema.Schema) error {
	m.s = s
	return nil
}

// DropTable Drop table schema
func (m *Source) DropTable(t string) error {
	delete(m.raw, t)
	delete(m.tables, t)
	names := make([]string, 0, len(m.tables))
	for tableName, _ := range m.raw {
		names = append(names, tableName)
	}
	m.tablenamelist = names
	return nil
}

// Open connection to given tablename.
func (m *Source) Open(tableName string) (schema.Conn, error) {
	tableName = strings.ToLower(tableName)
	if ds, ok := m.tables[tableName]; ok {
		return &Table{StaticDataSource: ds}, nil
	}
	err := m.loadTable(tableName)
	if err != nil {
		u.Errorf("could not load table %q  err=%v", tableName, err)
		return nil, err
	}
	ds := m.tables[tableName]
	return &Table{StaticDataSource: ds}, nil
}

// Table get table schema for given table name.  If given table is not currently
// defined, will load, infer schema.
func (m *Source) Table(tableName string) (*schema.Table, error) {
	tableName = strings.ToLower(tableName)
	if ds, ok := m.tables[tableName]; ok {
		return ds.Table(tableName)
	}
	err := m.loadTable(tableName)
	if err != nil {
		u.Errorf("could not load table %q  err=%v", tableName, err)
		return nil, err
	}
	ds, ok := m.tables[tableName]
	if !ok {
		return nil, schema.ErrNotFound
	}
	return ds.Table(tableName)
}

func (m *Source) loadTable(tableName string) error {
	ds, err := NewDataSource(m.db, tableName)
	if err != nil {
		return err
	}
	m.tables[tableName] = ds
	iter := &Table{StaticDataSource: ds}
	tbl, err := ds.Table(tableName)
	if err != nil {
		return err
	}
	return datasource.IntrospectTable(tbl, iter)
}

// Close csv source.
func (m *Source) Close() error { return nil }

// Tables list of tables.
func (m *Source) Tables() []string { return m.tablenamelist }

// CreateTable create a csv table in this source.
func (m *Source) CreateTable(tableName string, data interface{}) error {
	if _, exists := m.tables[tableName]; exists {
		return nil
	}
	cols, err := struct2Fields(data)
	if err != nil {
		return err
	}
	m.db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("schema"))
		sch := dataSchema{
			IndexedCol: 0,
			Cols:       cols,
		}
		buf, _ := json.Marshal(sch)
		b.Put([]byte(tableName), buf)
		tx.CreateBucketIfNotExists([]byte(tableName))
		return nil
	})
	m.tablenamelist = append(m.tablenamelist, tableName)
	m.loadTable(tableName)
	schema.DefaultRegistry().SchemaRefresh(m.dbname)
	return nil
}
