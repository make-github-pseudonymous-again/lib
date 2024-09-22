package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/make-github-pseudonymous-again/npm-downloads/internals/arrays"
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

	ScopedPackagePrefix = "@"
	DateFormat          = "2006-01-02"
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

	db := dependencies.Storage()
	defer db.Close()

	period, packages := args()

	var wg sync.WaitGroup
	results := make(chan npm.SinglePackageResponse)
	errors := make(chan error)

	requestTime := time.Now()
	batches := packageDownloadBatches(packages)
	fetch(&wg, results, errors, period, batches)

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

func insertRecords(db *sql.DB, batchSize int, pkg npm.SinglePackageResponse, requestTime time.Time) error {
	fmt.Printf("Inserting records for %v\n", pkg.Package)

	for i := 0; i < len(pkg.Downloads); i += batchSize {
		var placeholders []string
		var args []interface{}

		j := min(i+batchSize, len(pkg.Downloads))

		for k, point := range pkg.Downloads[i:j] {
			date, err := time.Parse(DateFormat, point.Day)
			if err != nil {
				return fmt.Errorf("Error parsing date: %v", err)
			}
			year := date.Year()
			month := int(date.Month())
			day := date.Day()
			dayOfWeek := int(date.Weekday())

			fmt.Printf("BATCH add record for %v (%v, %v)\n", pkg.Package, point.Day, point.Downloads)

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

		query := fmt.Sprintf(
			DownloadsUpsertTemplate,
			strings.Join(placeholders, ","),
		)

		fmt.Printf("BATCH execute (%v)\n", j-i)

		_, err := db.Exec(query, args...)
		if err != nil {
			return fmt.Errorf("error executing batch insert query: %v", err)
		}

	}

	return nil
}

func isScopedPackageName(name string) bool {
	return strings.HasPrefix(name, ScopedPackagePrefix)
}

func packageDownloadBatches(packageNames []string) [][]string {
	// NOTE: Partition between scoped and non-scoped packages.
	var scopedPackages []string
	var nonScopedPackages []string

	for _, pkg := range packageNames {
		if isScopedPackageName(pkg) {
			scopedPackages = append(scopedPackages, pkg)
		} else {
			nonScopedPackages = append(nonScopedPackages, pkg)
		}
	}

	// NOTE: Group non-scoped packages into batches of 128.
	nonScopedBatches := arrays.Chunk(nonScopedPackages, 128)

	// NOTE: Return all batches.
	batches := nonScopedBatches
	for _, pkg := range scopedPackages {
		scopedBatch := []string{pkg}
		batches = append(batches, scopedBatch)
	}

	return batches
}

func fetch(wg *sync.WaitGroup, results chan<- npm.SinglePackageResponse, errors chan<- error, period string, batches [][]string) {
	for _, batch := range batches {
		wg.Add(1)
		go npm.FetchBatch(wg, results, errors, period, batch)
	}

	go func() {
		wg.Wait()
		close(results)
		close(errors)
	}()
}
