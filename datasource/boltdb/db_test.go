package boltdb

import (
	"database/sql"
	"log"
	"testing"

	"github.com/jinzhu/gorm"
)

type Person struct {
	ID      int64  `db:"id"`
	Name    string `db:"name"`
	Counter string `db:"counter"`
}

func (p *Person) TableName() string {
	return "hello"
}

func TestDB(t *testing.T) {
	ds, _ := CreateDB("abc")
	ds.CreateTable("hello", 0, []string{"id", "name", "counter"})
	originDB, _ := sql.Open("qlbridge", "abc")
	db, err := gorm.Open("mysql", originDB)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	db.LogMode(true)
	db.Create(&Person{ID: 1, Name: "world", Counter: "20"})
	db.Create(&Person{ID: 2, Name: "world", Counter: "30"})
	var people []Person
	err = db.Find(&people).Error
	log.Println(people, err)
}
