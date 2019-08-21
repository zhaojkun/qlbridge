// Membtree implements a Datasource in-memory implemenation
// using the google btree.
package boltdb

import (
	"bytes"
	"database/sql/driver"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	u "github.com/araddon/gou"
	"github.com/boltdb/bolt"
	"github.com/dchest/siphash"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

const (
	sourceType = "boltdb"
)

var (
	// Different Features of this Static Data Source
	_ schema.Source       = (*StaticDataSource)(nil)
	_ schema.Conn         = (*StaticDataSource)(nil)
	_ schema.ConnColumns  = (*StaticDataSource)(nil)
	_ schema.ConnScanner  = (*StaticDataSource)(nil)
	_ schema.ConnSeeker   = (*StaticDataSource)(nil)
	_ schema.ConnUpsert   = (*StaticDataSource)(nil)
	_ schema.ConnDeletion = (*StaticDataSource)(nil)
)

// Key implements Key and Sort interfaces.
type Key struct {
	Id uint64
}

func NewKey(key uint64) *Key     { return &Key{key} }
func (m *Key) Key() driver.Value { return driver.Value(m.Id) }

func makeId(dv driver.Value) uint64 {
	switch vt := dv.(type) {
	case int:
		return uint64(vt)
	case int64:
		return uint64(vt)
	case []byte:
		return siphash.Hash(0, 1, vt)
		// iv, err := strconv.ParseUint(string(vt), 10, 64)
		// if err != nil {
		// 	u.Warnf("could not create id: %v  for %v", err, dv)
		// }
		// return iv
	case string:
		return siphash.Hash(0, 1, []byte(vt))
		// iv, err := strconv.ParseUint(vt, 10, 64)
		// if err != nil {
		// 	u.Warnf("could not create id: %v  for %v", err, dv)
		// }
		// return iv
	case *Key:
		//u.Infof("got %#v", vt)
		return vt.Id
	case datasource.KeyCol:
		//u.Infof("got %#v", vt)
		return makeId(vt.Val)
	case nil:
		return 0
	default:
		u.LogTracef(u.WARN, "no id conversion for type")
		u.Warnf("not implemented conversion: %T", dv)
	}
	return 0
}

// StaticDataSource implements qlbridge DataSource to allow in memory native go data
// to have a Schema and implement and be operated on by Sql Operations
//
// Features
// - only a single column may (and must) be identified as the "Indexed" column
// - NOT threadsafe
// - each StaticDataSource = a single Table
type StaticDataSource struct {
	exit       <-chan bool
	name       string
	tbl        *schema.Table
	indexCol   int // Which column position is indexed?  ie primary key
	cursor     uint64
	db         *bolt.DB
	bucketName string
	max        int
}

type Field struct {
	Name    string
	ValType value.ValueType
	Size    int
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	gob.Register(time.Time{})
}

type dataSchema struct {
	IndexedCol int     `json:"indexed_col"`
	Cols       []Field `json:"cols"`
}

func NewDataSource(db *bolt.DB, name string) (*StaticDataSource, error) {
	var sch dataSchema
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("schema"))
		v := b.Get([]byte(name))
		if v == nil {
			return errors.New("not found")
		}
		err := json.Unmarshal(v, &sch)
		return err
	})
	if err != nil {
		return nil, err
	}
	tbl := schema.NewTable(name)
	m := StaticDataSource{indexCol: sch.IndexedCol, name: name}
	m.db = db
	m.tbl = tbl
	m.bucketName = name
	var cols []string
	for _, field := range sch.Cols {
		tbl.AddField(schema.NewFieldBase(field.Name, field.ValType, field.Size, ""))
		cols = append(cols, field.Name)
	}
	m.tbl.SetColumns(cols)
	if err := datasource.IntrospectTable(m.tbl, m.CreateIterator()); err != nil {
		u.Errorf("Could not introspect schema %v", err)
	}
	return &m, nil
}

func NewStaticDataSource(name string, indexedCol int, data [][]driver.Value, cols []string) *StaticDataSource {
	dbpath := fmt.Sprintf("%s.db", name)
	os.Remove(dbpath)
	db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Panic(err)
	}
	// This source schema is a single table
	tbl := schema.NewTable(name)
	m := StaticDataSource{indexCol: indexedCol, name: name}
	m.db = db
	m.tbl = tbl
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucket([]byte(name))
		return nil
	})
	m.bucketName = name
	m.tbl.SetColumns(cols)
	for _, row := range data {
		m.Put(nil, nil, row)
	}
	if err := datasource.IntrospectTable(m.tbl, m.CreateIterator()); err != nil {
		u.Errorf("Could not introspect schema %v", err)
	}
	return &m
}

// StaticDataValue is used to create a static name=value pair that matches
// DataSource interfaces
func NewStaticDataValue(name string, data interface{}) *StaticDataSource {
	row := []driver.Value{data}
	ds := NewStaticDataSource(name, 0, [][]driver.Value{row}, []string{name})
	return ds
}

func NewStaticData(name string) *StaticDataSource {
	return NewStaticDataSource(name, 0, make([][]driver.Value, 0), nil)
}

func (m *StaticDataSource) Init()                                     {}
func (m *StaticDataSource) Setup(*schema.Schema) error                { return nil }
func (m *StaticDataSource) Open(connInfo string) (schema.Conn, error) { return m, nil }
func (m *StaticDataSource) Table(table string) (*schema.Table, error) { return m.tbl, nil }
func (m *StaticDataSource) Close() error                              { return nil }
func (m *StaticDataSource) CreateIterator() schema.Iterator           { return m }
func (m *StaticDataSource) Tables() []string                          { return []string{m.name} }
func (m *StaticDataSource) Columns() []string                         { return m.tbl.Columns() }
func (m *StaticDataSource) Length() int {
	var count int
	m.rangeloop(func(id uint64, row []driver.Value, colidx map[string]int) bool {
		count++
		return true
	})
	return count
}

func (m *StaticDataSource) SetColumns(cols []string) { m.tbl.SetColumns(cols) }

func (m *StaticDataSource) Next() schema.Message {
	//u.Infof("Next()")
	select {
	case <-m.exit:
		return nil
	default:
		for {
			nextID, row, colIdx, err := m.getNext(m.cursor)
			m.max++
			// if m.max > 20 {
			// 	return nil
			// }
			if err != nil {
				m.cursor = 0
				return nil
			}
			m.cursor = nextID

			return datasource.NewSqlDriverMessageMap(nextID, row, colIdx)
		}
	}
}

// interface for Upsert.Put()
func (m *StaticDataSource) Put(ctx context.Context, key schema.Key, row interface{}) (schema.Key, error) {
	//u.Infof("%p Put(),  row:%#v", m, row)
	switch rowVals := row.(type) {
	case []driver.Value:
		if len(rowVals) != len(m.Columns()) {
			u.Warnf("wrong column ct")
			return nil, fmt.Errorf("Wrong number of columns, got %v expected %v", len(rowVals), len(m.Columns()))
		}
		id := makeId(rowVals[m.indexCol])
		m.put(id, rowVals, m.tbl.FieldPositions)
		return NewKey(id), nil
	case map[string]driver.Value:
		// We need to convert the key:value to []driver.Value so
		// we need to look up column index for each key, and write to vals

		// TODO:   if this is a partial update, we need to look up vals
		row := make([]driver.Value, len(m.Columns()))
		if len(rowVals) < len(m.Columns()) {
			// How do we get the key?
			//m.Get(key)
		}

		for key, val := range rowVals {
			if keyIdx, ok := m.tbl.FieldPositions[key]; ok {
				row[keyIdx] = val
			} else {
				return nil, fmt.Errorf("Found column in Put that doesn't exist in cols: %v", key)
			}
		}
		id := uint64(0)
		if key == nil {
			if row[m.indexCol] == nil {
				// Since we do not have an indexed column to work off of,
				// the ideal would be to get the job builder/planner to do
				// a scan with whatever info we have and feed that in?   Instead
				// of us implementing our own scan?
				u.Warnf("wtf, nil key? %v %v", m.indexCol, row)
				return nil, fmt.Errorf("cannot update on non index column ")
			}
			id = makeId(row[m.indexCol])
		} else {
			id = makeId(key)
			sdm, _ := m.Get(key)
			//u.Debugf("sdm: %#v  err%v", sdm, err)
			if sdm != nil {
				if dmval, ok := sdm.Body().(*datasource.SqlDriverMessageMap); ok {
					for i, val := range dmval.Values() {
						if row[i] == nil {
							row[i] = val
						}
					}
				}
			}
		}
		m.put(id, row, m.tbl.FieldPositions)
		return NewKey(id), nil
	default:
		u.Warnf("not implemented %T", row)
		return nil, fmt.Errorf("Expected []driver.Value but got %T", row)
	}
}

func (m *StaticDataSource) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *StaticDataSource) Get(key driver.Value) (schema.Message, error) {
	id := makeId(key)
	row, colidx, err := m.get(id)
	if err == nil {
		return datasource.NewSqlDriverMessageMap(id, row, colidx), nil
	}
	return nil, schema.ErrNotFound // Should not found be an error?
}

func (m *StaticDataSource) MultiGet(keys []driver.Value) ([]schema.Message, error) {
	rows := make([]schema.Message, len(keys))
	for i, key := range keys {
		item, _ := m.Get(key)
		if item == nil {
			return nil, schema.ErrNotFound
		}
		rows[i] = item
	}
	return rows, nil
}

// Interface for Deletion
func (m *StaticDataSource) Delete(key driver.Value) (int, error) {
	err := m.delete(makeId(key))
	if err != nil {
		return 0, schema.ErrNotFound
	}
	return 1, nil
}

// DeleteExpression Delete using a Where Expression
func (m *StaticDataSource) DeleteExpression(p interface{}, where expr.Node) (int, error) {
	_, ok := p.(*plan.Delete)
	if !ok {
		return 0, plan.ErrNoPlan
	}

	deletedKeys := make([]*Key, 0)
	m.rangeloop(func(id uint64, row []driver.Value, colIdx map[string]int) bool {
		msgCtx := datasource.NewSqlDriverMessageMap(id, row, colIdx)
		whereValue, ok := vm.Eval(msgCtx, where)
		if !ok {
			u.Debugf("could not evaluate where: %v", msgCtx.Values())
			//return deletedCt, fmt.Errorf("Could not evaluate where clause")
			return true
		}
		switch whereVal := whereValue.(type) {
		case value.BoolValue:
			if whereVal.Val() == false {
				//this means do NOT delete
			} else {
				// Delete!
				indexVal := msgCtx.Values()[m.indexCol]
				deletedKeys = append(deletedKeys, NewKey(makeId(indexVal)))
			}
		case nil:
			// ??
		default:
			if whereVal.Nil() {
				// Doesn't match, so don't delete
			} else {
				u.Warnf("unknown type? %T", whereVal)
			}
		}
		return true
	})

	for _, deleteKey := range deletedKeys {
		if ct, err := m.Delete(deleteKey); err != nil {
			u.Errorf("Could not delete key: %v", deleteKey)
		} else if ct != 1 {
			u.Errorf("delete should have removed 1 key %v", deleteKey)
		}
	}
	return len(deletedKeys), nil
}

type itemData struct {
	ID       uint64
	Row      []driver.Value
	ColIndex map[string]int
}

func unmarshalItem(val []byte) (*itemData, error) {
	var item itemData
	dec := gob.NewDecoder(bytes.NewReader(val))
	err := dec.Decode(&item)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func marshalItem(item *itemData) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	enc.Encode(item)
	buf := buffer.Bytes()
	return buf
}

func (m *StaticDataSource) put(id uint64, row []driver.Value, colindex map[string]int) error {
	key := fmt.Sprint(id)
	buf := marshalItem(&itemData{
		ID:       id,
		Row:      row,
		ColIndex: colindex,
	})
	err := m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(m.bucketName))
		err := b.Put([]byte(key), buf)
		return err
	})
	return err
}

func (m *StaticDataSource) get(id uint64) (row []driver.Value, colidx map[string]int, err error) {
	var item *itemData
	key := fmt.Sprint(id)
	m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(m.bucketName))
		v := b.Get([]byte(key))
		item, err = unmarshalItem(v)
		return err
	})
	if err != nil {
		return
	}
	return item.Row, item.ColIndex, nil
}

func (m *StaticDataSource) delete(id uint64) error {
	key := fmt.Sprint(id)
	err := m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(m.bucketName))
		v := b.Get([]byte(key))
		if v == nil {
			return errors.New("not found")
		}
		return b.Delete([]byte(key))
	})
	return err
}

func (m *StaticDataSource) rangeloop(fn func(id uint64, row []driver.Value, colidx map[string]int) bool) {
	m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(m.bucketName))
		b.ForEach(func(k, v []byte) error {
			item, err := unmarshalItem(v)
			if err != nil {
				return err
			}
			fn(item.ID, item.Row, item.ColIndex)
			return nil
		})
		return nil
	})
}

func (m *StaticDataSource) getNext(id uint64) (nextID uint64, row []driver.Value, colIdx map[string]int, err error) {
	var dstKey, dstVal []byte
	m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(m.bucketName))
		c := b.Cursor()
		key, val := c.First()
		if id != 0 {
			targetId := fmt.Sprint(id)
			c.Seek([]byte(targetId))
			key, val = c.Next()
		}
		dstKey = make([]byte, len(key), len(key))
		copy(dstKey, key)
		dstVal = make([]byte, len(val), len(val))
		copy(dstVal, val)
		return nil
	})
	nextid, _ := strconv.Atoi(string(dstKey))
	nextID = uint64(nextid)
	item, err := unmarshalItem(dstVal)
	if err != nil {
		return
	}
	return nextID, item.Row, item.ColIndex, nil
}
