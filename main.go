package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

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

	DateFormat                       = "2006-01-02"
	NPM_DOWNLOADS_API                = "https://api.npmjs.org"
	NPM_DOWNLOADS_API_RANGE_ENDPOINT = "%s/downloads/range/%s/%s"

	LastDay   = "last-day"
	LastWeek  = "last-week"
	LastMonth = "last-month"
	LastYear  = "last-year"

// NOTE: Can also have the form YYYY-MM-DD or YYYY-MM-DD:YYYY-MM-DD
)

const (
	SingleResponse = iota
	MultiResponse
)

// Struct for daily downloads
type DailyDownload struct {
	Downloads int    `json:"downloads"`
	Day       string `json:"day"`
}

// Struct for a single-package response
type SinglePackageResponse struct {
	Start     string          `json:"start"`
	End       string          `json:"end"`
	Package   string          `json:"package"`
	Downloads []DailyDownload `json:"downloads"`
}

// Struct for a multi-package response
type MultiPackageResponse map[string]SinglePackageResponse

func fetchJSON(
	wg *sync.WaitGroup,
	resultsChan chan<- SinglePackageResponse,
	errorsChan chan<- error,
	url string,
	responseType int) {
	defer wg.Done()

	fmt.Printf("FETCH %s\n", url)

	resp, err := http.Get(url)
	if err != nil {
		errorsChan <- err
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		errorsChan <- err
		return
	}

	// Parse based on the specified response type
	if responseType == MultiResponse {
		var multiResp MultiPackageResponse
		if err := json.Unmarshal(body, &multiResp); err == nil {
			// Send each single response to resultsChan
			for _, singleResp := range multiResp {
				resultsChan <- singleResp
			}
			return
		}
		errorsChan <- fmt.Errorf("failed to parse multi-package response: %s", body)
	} else if responseType == SingleResponse {
		var singleResp SinglePackageResponse
		if err := json.Unmarshal(body, &singleResp); err == nil && singleResp.Package != "" {
			resultsChan <- singleResp
			return
		}
		errorsChan <- fmt.Errorf("failed to parse single-package response: %s", body)
	}

	// If the response type doesn't match any case, log an error
	errorsChan <- fmt.Errorf("unknown response type: %d", responseType)
}

func storage() *sql.DB {
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

func args() (string, []string) {
	var period string
	flag.StringVar(&period, "period", LastDay, "Period to fetch")
	flag.Parse()
	fmt.Printf("Period: %v\n", period)
	packages := flag.Args()
	fmt.Printf("Packages: %v\n", packages)
	return period, packages
}

func main() {

	db := storage()
	defer db.Close()

	period, packages := args()

	var wg sync.WaitGroup
	results := make(chan SinglePackageResponse)
	errors := make(chan error)

	requestTime := time.Now()
	fetch(&wg, results, errors, period, packages)

	failures := 0

	for {
		select {
		case result, ok := <-results:
			if ok {
				err := insertRecords(db, 100, result, requestTime)
				if err != nil {
					log.Fatalf("Error inserting record: %v\n", err)
				}
			} else {
				results = nil
			}
		case err, ok := <-errors:
			if ok {
				log.Printf("Error occurred during fetch: %v", err)
				failures += 1
			} else {
				errors = nil
			}
		}
		if results == nil && errors == nil {
			break
		}
	}

	fmt.Printf("Failures: %v\n", failures)

	fmt.Println("DONE")
}

func insertRecords(db *sql.DB, batchSize int, pkg SinglePackageResponse, requestTime time.Time) error {
	fmt.Printf("Inserting records for %v\n", pkg.Package)

	// SQL query for batch inserts
	insertQuery := `INSERT INTO downloads (
		name, count, date, last_updated_at,
		date_year, date_month, date_day, date_day_of_week
	) VALUES %s
	ON CONFLICT(name, date_year, date_month, date_day)
	DO UPDATE SET
		count=excluded.count,
		last_updated_at=excluded.last_updated_at
	WHERE
		excluded.count > downloads.count;`

	// Create a slice to hold value placeholders and arguments
	var placeholders []string
	var args []interface{}

	for i := 0; i < len(pkg.Downloads); i += batchSize {
		j := i + batchSize
		if j > len(pkg.Downloads) {
			j = len(pkg.Downloads)
		}

		for k, point := range pkg.Downloads[i:j] {
			// Extract components of the requestTime (e.g., year, month, day)
			date, err := time.Parse(DateFormat, point.Day)
			if err != nil {
				return fmt.Errorf("Error parsing date: %v", err)
			}
			year := date.Year()
			month := int(date.Month())
			day := date.Day()
			dayOfWeek := int(date.Weekday())

			fmt.Printf("BATCH add record for %v (%v, %v)\n", pkg.Package, point.Day, point.Downloads)

			// Append placeholders like ($1, $2, $3, $4, ...)
			placeholders = append(
				placeholders,
				fmt.Sprintf(
					"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
					k*8+1, k*8+2, k*8+3, k*8+4, k*8+5, k*8+6, k*8+7, k*8+8,
				),
			)

			// Append the actual values to the args slice
			args = append(args,
				pkg.Package,     // $1: name
				point.Downloads, // $2: count
				date,            // $3: date
				requestTime,     // $4: last_updated_at
				year,            // $5: date_year
				month,           // $6: date_month
				day,             // $7: date_day
				dayOfWeek,       // $8: date_day_of_week
			)
		}

		// Join the placeholders to create the final query
		finalQuery := fmt.Sprintf(insertQuery, strings.Join(placeholders, ","))

		fmt.Println("BATCH execute")

		// Execute the batch insert
		_, err := db.Exec(finalQuery, args...)
		if err != nil {
			return fmt.Errorf("error executing batch insert query: %v", err)
		}

	}

	return nil
}

func chunkArray(array []string, size int) [][]string {
	var chunks [][]string
	for i := 0; i < len(array); i += size {
		end := i + size
		if end > len(array) {
			end = len(array)
		}
		chunks = append(chunks, array[i:end])
	}
	return chunks
}

func fetch(wg *sync.WaitGroup, resultsChan chan<- SinglePackageResponse, errorsChan chan<- error, period string, packageNames []string) {

	// Separate scoped and non-scoped packages
	var scopedPackages []string
	var nonScopedPackages []string

	for _, pkg := range packageNames {
		if strings.HasPrefix(pkg, "@") {
			scopedPackages = append(scopedPackages, pkg)
		} else {
			nonScopedPackages = append(nonScopedPackages, pkg)
		}
	}

	// Group non-scoped packages into chunks of 128
	nonScopedChunks := chunkArray(nonScopedPackages, 128)

	allChunks := nonScopedChunks
	for _, pkg := range scopedPackages {
		allChunks = append(allChunks, []string{pkg})
	}

	for _, chunk := range allChunks {
		wg.Add(1)
		namesJoined := strings.Join(chunk, ",")
		url := fmt.Sprintf(NPM_DOWNLOADS_API_RANGE_ENDPOINT, NPM_DOWNLOADS_API, period, namesJoined)
		responseType := SingleResponse
		if len(chunk) >= 2 {
			responseType = MultiResponse
		}
		go fetchJSON(
			wg,
			resultsChan,
			errorsChan,
			url,
			responseType,
		)
	}

	// Wait for all goroutines to finish and close channels
	go func() {
		wg.Wait()
		close(resultsChan)
		close(errorsChan)
	}()
}
