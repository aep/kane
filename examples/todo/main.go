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

func (t *User) PK() any {
	return t.ID
}

type Todo struct {
	UserID string
	Order  int
	Text   string
	Done   bool
}

func (t *Todo) PK() any {
	// pitfall i need to document: forgetting a separator can lead to injection attacks
	return fmt.Sprintf("%s/%d", t.UserID, t.Order)
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
	check(db.Get(context.TODO(), &rval, kane.Eq("ID", "bob")))
	fmt.Println(rval.Name)

	// get the full document
	rdoc := kane.StoredDocument{Val: &User{}}
	check(db.Get(context.TODO(), &rdoc, kane.Eq("ID", "bob")))
	fmt.Println(rdoc.Val.(*User).Name)

	// find the document by name
	var rval2 User
	check(db.Get(context.TODO(), &rval2, kane.Eq("Name", "Bob Baumeister")))
	fmt.Println(rval2.Name)

	// find the document by age
	var rval3 User
	check(db.Get(context.TODO(), &rval3, kane.Eq("Age", 0xff)))
	fmt.Println(rval3.Name)

	check(db.Set(context.TODO(), &Todo{
		UserID: "bob",
		Order:  2,
		Text:   "clean the balkony",
	}))
	check(db.Set(context.TODO(), &Todo{
		UserID: "bob",
		Order:  6666666,
		Text:   "make money",
	}))
	check(db.Set(context.TODO(), &Todo{
		UserID: "bob",
		Order:  1,
		Text:   "wake up",
	}))
	check(db.Set(context.TODO(), &Todo{
		UserID: "bobi",
		Order:  1,
		Text:   "clean the balkony",
	}))
	check(db.Set(context.TODO(), &Todo{
		UserID: "boc",
		Order:  1,
		Text:   "clean the balkony",
	}))

	fmt.Println("---")

	for o, err := range kane.Iter[Todo](context.TODO(), db, kane.Eq("UserID", "bob")) {
		check(err)
		fmt.Println(o)
	}

	fmt.Println("---")

	for o, err := range kane.Iter[Todo](context.TODO(), db, kane.Has("Order")) {
		check(err)
		fmt.Println(o)
	}
	fmt.Println("---")

	check(db.Del(context.TODO(), &Todo{
		UserID: "bob",
		Order:  1,
	}))
	check(db.Del(context.TODO(), &Todo{
		UserID: "bob",
		Order:  2,
	}))

	for o, err := range kane.Iter[Todo](context.TODO(), db, kane.Eq("UserID", "bob")) {
		check(err)
		fmt.Println(o)
	}
}
