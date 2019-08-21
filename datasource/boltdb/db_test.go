package boltdb

import (
	"database/sql"
	"log"
	"testing"

	"github.com/jinzhu/gorm"
)

type Person struct {
	ID   string `db:"id"`
	Name string `db:"name"`
}

func (p *Person) TableName() string {
	return "hello"
}

func TestDB(t *testing.T) {
	ds, err := CreateDB("abc")
	log.Println(ds, err)
	ds.CreateTable("hello", 0, []string{"id", "name"})
	originDB, _ := sql.Open("qlbridge", "abc")
	db, err := gorm.Open("mysql", originDB)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	log.Println(db)
	db.Create(&Person{ID: "1", Name: "world"})
	db.Create(&Person{ID: "2", Name: "world"})
	var people []Person
	db.LogMode(true)
	err = db.Find(&people).Error
	log.Println(people, err)
}
