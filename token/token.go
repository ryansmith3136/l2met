package token

import "fmt"

type Token struct {
	Id   string
	User string
	Pass string
}

func (t *Token) Get() {
	rows, err := pg.Query("select u, p from tokens where id = $1", t.Id)
	if err != nil {
		fmt.Printf("error=%s\n", err)
		return
	}
	defer rows.Close()
	rows.Next()
	rows.Scan(&t.User, &t.Pass)
	return
}
