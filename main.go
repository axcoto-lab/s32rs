package main

func main() {
	qe := initQueue()
	db := initDB()

	a := &app{
		Qe: qe,
		DB: db,
	}

	initServer(a)
}
