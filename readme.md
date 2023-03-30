# PGX

Simple PostgreSQL ORM extension for Golang

# Install

```go
go get -u github.com/StevenZack/pgx
```

# Example

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/StevenZack/pgx"
	"github.com/lib/pq"
)

type User struct {
	Id         uint32         `db:"id"`                       // serial
	Name       string         `db:"name" limit:"32" index:""` // varchar(32) with index
	Token      string         `db:"token" length:"32"`        // char(32)
	Content    string         `db:"content"`                  // text
	Tags       pq.StringArray `db:"tags"`                     // text[]
	UpdateTime time.Time      `db:"update_time"`              // timestamp
	CreateTime time.Time      `db:"create_time"`              //timestamp
}

const (
	dsn = `dbname=postgres user=postgres password=123456 host=localhost port=5432 sslmode=disable`
)

func main() {
	m, e := pgx.NewBaseModel(dsn, User{})
	if e != nil {
		log.Println(e)
		return
	}

	id, e := m.Insert(User{
		Name:       "Bob",
		Token:      "12345678901234567890123456789012",
		Content:    "asd",
		Tags:       []string{"male"},
		UpdateTime: time.Now(),
		CreateTime: time.Now(),
	})
	if e != nil {
		log.Fatal(e)
	}
	fmt.Println("inserted id=", id)

	// find one user
	user, e := m.FindWhere("id=$1", id.(uint32))
	if e != nil {
		log.Fatal(e)
	}
	fmt.Println("findWhere: ", user.(*User))

	// find one user using index: 'name'
	user, e = m.FindWhere("name=$1", "Bob")
	if e != nil {
		log.Fatal(e)
	}
	fmt.Println("findWhere by index:", user.(*User))

	// query multiple users
	users, e := m.QueryWhere("tags && $1", pq.StringArray{"male"})
	if e != nil {
		log.Fatal(e)
	}
	for i, user := range users.([]*User) {
		fmt.Println(i, user)
	}
}

```