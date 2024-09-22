package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/make-github-pseudonymous-again/npm-downloads/internals/dependencies"
	"github.com/make-github-pseudonymous-again/npm-downloads/internals/npm"
)

const (
	DownloadsUpsertTemplate = `
	INSERT INTO downloads (
		name, count, date, last_updated_at,
		date_year, date_month, date_day, date_day_of_week
	) VALUES %s
	ON CONFLICT(name, date_year, date_month, date_day)
	DO UPDATE SET
		count=excluded.count,
		last_updated_at=excluded.last_updated_at
	WHERE
		excluded.count > downloads.count;
	`

	DateFormat = "2006-01-02"
	// TODO: https://api.npmjs.org/versions/{url-encoded-/ package name}/last-week
	// NOTE: https://github.com/npm/registry/blob/main/docs/download-counts.md#per-version-download-counts

	// NOTE: Can also have the form YYYY-MM-DD or YYYY-MM-DD:YYYY-MM-DD
	LastDay   = "last-day"
	LastWeek  = "last-week"
	LastMonth = "last-month"
	LastYear  = "last-year"
)

const (
	SingleResponse = iota
	MultiResponse
)

func args() (int, string, []string) {
	var period string
	flag.StringVar(&period, "period", LastDay, "Period to fetch")
	batch := flag.Int("batch", 100, "Batch size for DB inserts")
	flag.Parse()
	fmt.Printf("Period: %v\n", period)
	packages := flag.Args()
	fmt.Printf("Packages: %v\n", packages)
	return *batch, period, packages
}

func main() {
	db := dependencies.Storage()
	defer db.Close()

	batch, period, packages := args()

	var fetchWaitGroup sync.WaitGroup
	fetchResults := make(chan npm.SinglePackageResponse)
	fetchErrors := make(chan error)

	requestTime := time.Now()
	batches := npm.PackageDownloadBatches(period, packages)
	scheduleFetches(&fetchWaitGroup, fetchResults, fetchErrors, batches)

	var insertWaitGroup sync.WaitGroup
	// NOTE: We only allow one insert at a time since there is no possible
	// concurrency with sqlite3 and the pre-processing is quite ligth.
	insertSemaphore := make(chan struct{}, 1)
	insertErrors := make(chan error)

	failures := 0

	var insertLoopWaitGroup sync.WaitGroup
	insertLoopWaitGroup.Add(1)
	go func() {
		defer insertLoopWaitGroup.Done()
		for {
			select {
			case result, ok := <-fetchResults:
				if ok {
					scheduleInserts(
						&insertWaitGroup,
						insertSemaphore,
						insertErrors,
						db,
						batch,
						result,
						requestTime,
					)
				} else {
					fetchResults = nil
				}
			case err, ok := <-fetchErrors:
				if ok {
					log.Printf("Error occurred during fetch: %v\n", err)
					failures += 1
				} else {
					fetchErrors = nil
				}
			}

			if fetchResults == nil && fetchErrors == nil {
				break
			}
		}
	}()

	var insertLoopErrorsWaitGroup sync.WaitGroup
	insertLoopErrorsWaitGroup.Add(1)
	go func() {
		defer insertLoopErrorsWaitGroup.Done()
		for {
			select {
			case err, ok := <-insertErrors:
				if ok {
					log.Printf("Error occurred during insert: %v\n", err)
					failures += 1
				} else {
					insertErrors = nil
				}
			}

			if insertErrors == nil {
				break
			}
		}
	}()

	fmt.Println("WAIT fetch")
	fetchWaitGroup.Wait()
	close(fetchResults)
	close(fetchErrors)
	fmt.Println("WAIT insert loop")
	insertLoopWaitGroup.Wait()
	fmt.Println("WAIT insert")
	insertWaitGroup.Wait()
	close(insertSemaphore)
	close(insertErrors)
	fmt.Println("WAIT insert loop errors")
	insertLoopErrorsWaitGroup.Wait()
	fmt.Printf("Failures: %v\n", failures)
	fmt.Println("DONE")
}

func createBatchArgs(requestTime time.Time, name string, batch []npm.DailyDownload) ([]string, []interface{}) {
	var placeholders []string
	var args []interface{}

	for k, point := range batch {
		date, err := time.Parse(DateFormat, point.Day)
		if err != nil {
			log.Printf("Error parsing date: %v\n", err)
		}
		year := date.Year()
		month := int(date.Month())
		day := date.Day()
		dayOfWeek := int(date.Weekday())

		fmt.Printf("BATCH add record for %v (%v, %v)\n", name, point.Day, point.Downloads)

		placeholders = append(
			placeholders,
			fmt.Sprintf(
				// NOTE: ($1, $2, $3, $4, ...)
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				k*8+1, k*8+2, k*8+3, k*8+4, k*8+5, k*8+6, k*8+7, k*8+8,
			),
		)

		// Append the actual values to the args slice
		args = append(args,
			name,            // $1: name
			point.Downloads, // $2: count
			date,            // $3: date
			requestTime,     // $4: last_updated_at
			year,            // $5: date_year
			month,           // $6: date_month
			day,             // $7: date_day
			dayOfWeek,       // $8: date_day_of_week
		)
	}

	return placeholders, args
}

func scheduleInserts(
	wg *sync.WaitGroup,
	semaphore chan struct{},
	errors chan<- error,
	db *sql.DB,
	batchSize int,
	pkg npm.SinglePackageResponse,
	requestTime time.Time,
) {
	fmt.Printf("INSERT schedule %v\n", pkg.Package)

	for i := 0; i < len(pkg.Downloads); i += batchSize {
		j := min(i+batchSize, len(pkg.Downloads))

		wg.Add(1)
		go func(i int, j int) {
			defer wg.Done()
			semaphore <- struct{}{}        // NOTE: Acquire worker slot.
			defer func() { <-semaphore }() // NOTE: Release worker slot.

			batch := pkg.Downloads[i:j]

			placeholders, args := createBatchArgs(requestTime, pkg.Package, batch)

			query := fmt.Sprintf(
				DownloadsUpsertTemplate,
				strings.Join(placeholders, ","),
			)

			fmt.Printf("BATCH execute (%v)\n", j-i)

			_, err := db.Exec(query, args...)
			if err != nil {
				errors <- fmt.Errorf("error executing batch insert query: %v", err)
			}
		}(i, j)

	}
}

func scheduleFetches(
	wg *sync.WaitGroup,
	results chan<- npm.SinglePackageResponse,
	errors chan<- error,
	batches []npm.Batch,
) {
	for _, batch := range batches {
		wg.Add(1)
		go npm.FetchBatch(
			wg,
			results,
			errors,
			batch,
		)
	}
}
