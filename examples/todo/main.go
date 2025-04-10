package main

import (
	"context"
	"fmt"

	"github.com/aep/kane"
)

type User struct {
	ID   string
	Name string
	Age  int
}

type Todo struct {
	ID     int
	UserID string
	Text   string
	Done   bool
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	db, err := kane.Init("tikv://localhost:2379")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	check(db.Set(context.TODO(), &User{
		ID:   "bob",
		Age:  0xff,
		Name: "Bob Baumeister",
	}))

	// just get the val
	var rval User
	check(db.Get(context.TODO(), &rval, "ID", kane.Eq("bob")))
	fmt.Println(rval.Name)

	// get the full document
	rdoc := kane.Document{Val: &User{}}
	check(db.Get(context.TODO(), &rdoc, "ID", kane.Eq("bob")))
	fmt.Println(rdoc.Val.(*User).Name)

	// find the document by name
	var rval2 User
	check(db.Get(context.TODO(), &rval2, "Name", kane.Eq("Bob Baumeister")))
	fmt.Println(rval2.Name)

	// find the document by age
	var rval3 User
	check(db.Get(context.TODO(), &rval3, "Age", kane.Eq(0xff)))
	fmt.Println(rval3.Name)

	check(db.Set(context.TODO(), &Todo{
		ID:     12,
		UserID: "bob",
		Text:   "clean the balkony",
	}))
	check(db.Set(context.TODO(), &Todo{
		ID:     13,
		UserID: "bobi",
		Text:   "clean the balkony",
	}))
	check(db.Set(context.TODO(), &Todo{
		ID:     14,
		UserID: "boc",
		Text:   "clean the balkony",
	}))

	for o, err := range kane.Iter[Todo](context.TODO(), db, "UserID", kane.Eq("bob")) {
		check(err)
		fmt.Println(o)
	}
}
