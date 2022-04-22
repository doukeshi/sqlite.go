package main

import (
	"context"
	"database/sql"
	"log"
	"strconv"

	"github.com/doukeshi/sqlite.go"
)

// example from https://golang.org/s/sqlwiki
const (
	CREATE_SQL = `CREATE TABLE users(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name VARCHAR(64) NOT NULL,
		age INTEGER NOT NULL
	)`
	INSERT_SQL     = "INSERT INTO users(name, age) VALUES(?, ?)"
	SELECT_SQL     = "SELECT name FROM users WHERE age = ? "
	SELECT_ALL_SQL = "SELECT name, age FROM users"
	NAME           = "foobar"
	AGE            = 13
)

func main() {
	db, err := sql.Open(sqlite.DRIVER, ":memory:")
	if err != nil {
		log.Fatal("sql.Open, err: ", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal("db.PingContext, err: ", err)
	}

	re, err := db.ExecContext(ctx, CREATE_SQL)
	if err != nil {
		log.Fatal("db.ExecContext, err: ", err)
	}
	log.Printf("db.ExecContext, sql:[%s], result: %v", CREATE_SQL, re)

	re, err = db.ExecContext(ctx, INSERT_SQL, NAME, AGE)
	if err != nil {
		log.Fatal("db.ExecContext, err: ", err)
	}
	log.Printf("db.ExecContext, sql:[%s], result: %v", INSERT_SQL, re)

	for i := 0; i < 10; i++ {
		_, err = db.ExecContext(ctx, INSERT_SQL, NAME+strconv.FormatInt(int64(i), 10), AGE+i)
		if err != nil {
			log.Fatal("db.ExecContext, err: ", err)
		}
	}

	rows, err := db.QueryContext(ctx, SELECT_ALL_SQL)
	if err != nil {
		log.Fatal("db.QueryContext, err: ", err)
	}
	log.Printf("db.QueryContext, sql:[%s], rows: %+v", SELECT_ALL_SQL, rows)
	processAllRows(rows)

	stmt, err := db.PrepareContext(ctx, SELECT_SQL)
	if err != nil {
		log.Fatal("db.PrepareContext, err: ", err)
	}
	log.Printf("db.PrepareContext, sql:[%s], stmt: %+v", SELECT_SQL, stmt)
	rows, err = stmt.Query(AGE)
	log.Printf("db.QueryContext, sql:[%s], rows: %+v", SELECT_SQL, rows)
	if err != nil {
		log.Fatal("stmt.Query, err: ", err)
	}
	processRows(rows)
}

func processRows(rows *sql.Rows) {
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal("rows.Scan, err: ", err)
		}
		log.Printf("rows.Scan, name: %s", name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal("rows.Err: ", err)
	}
}

type user struct {
	name string
	age  int
}

func processAllRows(rows *sql.Rows) {
	var userArr []user
	defer rows.Close()
	for rows.Next() {
		var u user
		if err := rows.Scan(&u.name, &u.age); err != nil {
			log.Fatal("rows.Scan, err: ", err)
		}
		log.Printf("rows.Scan, u: %+v", u)
		userArr = append(userArr, u)
	}
	if err := rows.Err(); err != nil {
		log.Fatal("rows.Err: ", err)
	}
	log.Printf("processAllRows, userArr: %+v", userArr)
}
