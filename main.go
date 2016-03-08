package main

func main() {
	qe := initQueue()
	db := initDB()

	app := &App{
		Qe: qe,
		DB: db,
	}
	app.init()

	initWorker(app)
	initCert()
	initServer(app)
}
