package boltdb

import (
	"database/sql"
	"log"
	"testing"
	"time"

	"github.com/araddon/qlbridge/value"
	"github.com/jinzhu/gorm"
)

type Person struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Counter   string    `db:"counter"`
	Size      int       `db:"size"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

func (p *Person) TableName() string {
	return "hello"
}

func TestDB(t *testing.T) {
	ds, _ := CreateDB("abc")
	ds.CreateTable("hello", 0, []Field{
		Field{"id", value.IntType, 64},
		Field{"name", value.StringType, 64},
		Field{"counter", value.StringType, 64},
		Field{"size", value.IntType, 64},
		Field{"created_at", value.TimeType, 64},
		Field{"updated_at", value.TimeType, 64},
	})
	originDB, _ := sql.Open("qlbridge", "abc")
	db, err := gorm.Open("mysql", originDB)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	db.LogMode(true)
	db.Create(&Person{ID: 1, Name: "world", Counter: "abc", CreatedAt: time.Now()})
	db.Create(&Person{ID: 2, Name: "world", Counter: "", CreatedAt: time.Now()})
	db.Create(&Person{ID: 3, Name: "world", Counter: "20000", CreatedAt: time.Now()})
	db.Create(&Person{ID: 4, Name: "world4"})
	var people []Person
	err = db.Find(&people).Error
	log.Println(people, err)
}
