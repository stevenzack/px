# PGX

Simple PostgreSQL ORM extension for Golang

# Install

```go
go get -u github.com/stevenzack/px
```

# Example

```go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/stevenzack/px"
)

type User struct {
	Id          uint32                                               // serial not null primary key
	PhoneNumber sql.NullString    `limit:"36" index:"unique"` // varchar(36) unique index
	Info        map[string]string                                  // jsonb
	UpdateTime  time.Time         `index:""`
	CreateTime  time.Time         
}

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {

	urlExample := "postgres://stevenzack:@localhost:5432/langenius"
	c, e := px.NewBaseModel(urlExample, User{})
	if e != nil {
		log.Println(e)
		return
	}
	id, e := c.Insert(User{
		PhoneNumber: sql.NullString{Valid: true, String: "asd"},
	})
	if e != nil {
		log.Println(e)
		return
	}
	fmt.Println("inserted: ", id)

	v, e := c.Find(id)
	if e != nil {
		log.Println(e)
		return
	}
	fmt.Println(v)
}

```
