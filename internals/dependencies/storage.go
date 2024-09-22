package dependencies

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

const (
	StorageDriver = "sqlite3"
	StoragePath   = "./storage.sqlite3"

	DownloadsTable = `
	CREATE TABLE IF NOT EXISTS downloads (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		count INTEGER NOT NULL,
		date DATETIME NOT NULL,
    	last_updated_at DATETIME NOT NULL,

		date_year INTEGER NOT NULL,
		date_month INTEGER NOT NULL,
		date_day INTEGER NOT NULL,
		date_day_of_week INTEGER NOT NULL,

		UNIQUE(name, date_year, date_month, date_day)
	);
	`
)

func Storage() *sql.DB {
	// Connect to SQLite database (or create it if it doesn't exist)
	fmt.Println("Opening storage")
	db, err := sql.Open(StorageDriver, StoragePath)
	if err != nil {
		log.Fatalf("Error opening storage: %v\n", err)
	}

	fmt.Println("Creating table")
	_, err = db.Exec(DownloadsTable)
	if err != nil {
		log.Fatalf("Error creating table: %v\n", err)
	}

	return db
}
