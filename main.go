package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"slices"
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

type Values []string

// String is an implementation of the flag.Value interface
func (values *Values) String() string {
	return fmt.Sprintf("%v", *values)
}

// Set is an implementation of the flag.Value interface
func (values *Values) Set(value string) error {
	*values = append(*values, value)
	return nil
}

type Args struct {
	Batch       int
	Queue       int
	Period      string
	Authors     []string
	Scopes      []string
	Maintainers []string
	Keywords    []string
	Packages    []string
}

func _args() Args {
	var authors Values
	var scopes Values
	var maintainers Values
	var keywords Values
	period := flag.String("period", LastDay, "Period to fetch")
	flag.Var(&authors, "author", "Author queries")
	flag.Var(&scopes, "scope", "Scope queries")
	flag.Var(&maintainers, "maintainer", "Maintainer queries")
	flag.Var(&keywords, "keyword", "Keyword queries")
	batch := flag.Int("batch", 100, "Batch size for DB inserts")
	queue := flag.Int("queue", 2, "Queue size for API fetches")
	flag.Parse()
	packages := flag.Args()
	fmt.Printf("Batch: %v\n", *batch)
	fmt.Printf("Queue: %v\n", *queue)
	fmt.Printf("Period: %v\n", *period)
	fmt.Printf("Authors: %v\n", authors)
	fmt.Printf("Scopes: %v\n", scopes)
	fmt.Printf("Maintainers: %v\n", maintainers)
	fmt.Printf("Keywords: %v\n", keywords)
	fmt.Printf("Packages: %v\n", packages)
	return Args{
		*batch,
		*queue,
		*period,
		authors,
		scopes,
		maintainers,
		keywords,
		packages,
	}
}

func main() {
	db := dependencies.Storage()
	defer db.Close()

	args := _args()

	var searchWaitGroup sync.WaitGroup
	searchQueue := make(chan struct{}, args.Queue)
	searchResults := make(chan npm.SearchResponseObject)
	searchErrors := make(chan error)

	var queries []string
	for _, author := range args.Authors {
		query := fmt.Sprintf("author:%s", author)
		queries = append(queries, query)
	}
	for _, maintainer := range args.Maintainers {
		query := fmt.Sprintf("maintainer:%s", maintainer)
		queries = append(queries, query)
	}
	for _, scope := range args.Scopes {
		query := fmt.Sprintf("scope:%s", scope)
		queries = append(queries, query)
	}
	for _, keyword := range args.Keywords {
		query := fmt.Sprintf("keyword:%s", keyword)
		queries = append(queries, query)
	}
	scheduleSearches(&searchWaitGroup, searchQueue, searchResults, searchErrors, queries)

	var packages []string

	go func() {
		for {
			select {
			case result, ok := <-searchResults:
				if ok {
					log.Printf("Found: %v\n", result.Package.Name)
					packages = append(packages, result.Package.Name)
				} else {
					searchResults = nil
				}
			}

			if searchResults == nil {
				break
			}
		}
	}()

	fmt.Println("WAIT search")
	searchWaitGroup.Wait()
	fmt.Println("DONE search")

	for _, pkg := range args.Packages {
		packages = append(packages, pkg)
	}

	slices.Sort(packages)
	slices.Compact(packages)

	var fetchWaitGroup sync.WaitGroup
	fetchQueue := make(chan struct{}, args.Queue)
	fetchResults := make(chan npm.SinglePackageResponse)
	fetchErrors := make(chan error)

	requestTime := time.Now()
	batches := npm.PackageDownloadBatches(args.Period, packages)
	scheduleFetches(&fetchWaitGroup, fetchQueue, fetchResults, fetchErrors, batches)

	var insertWaitGroup sync.WaitGroup
	// NOTE: We only allow one insert at a time since there is no possible
	// concurrency with sqlite3 and the pre-processing is quite light.
	insertQueue := make(chan struct{}, 1)
	insertErrors := make(chan error)

	failures := 0

	var insertLoopWaitGroup sync.WaitGroup
	insertLoopWaitGroup.Add(2)
	go func() {
		defer insertLoopWaitGroup.Done()
		for {
			select {
			case result, ok := <-fetchResults:
				if ok {
					scheduleInserts(
						&insertWaitGroup,
						insertQueue,
						insertErrors,
						db,
						args.Batch,
						result,
						requestTime,
					)
				} else {
					fetchResults = nil
				}
			}

			if fetchResults == nil {
				break
			}
		}
	}()

	go func() {
		defer insertLoopWaitGroup.Done()
		for {
			select {
			case err, ok := <-fetchErrors:
				if ok {
					log.Printf("Error occurred during fetch: %v\n", err)
					failures += 1
				} else {
					fetchErrors = nil
				}
			}

			if fetchErrors == nil {
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
	close(insertQueue)
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
	queue chan struct{},
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
			queue <- struct{}{}        // NOTE: Acquire worker slot.
			defer func() { <-queue }() // NOTE: Release worker slot.

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
	queue chan struct{},
	results chan<- npm.SinglePackageResponse,
	errors chan<- error,
	batches []npm.Batch,
) {
	for _, batch := range batches {
		wg.Add(1)
		go func(batch npm.Batch) {
			defer wg.Done()
			queue <- struct{}{}        // NOTE: Acquire worker slot.
			defer func() { <-queue }() // NOTE: Release worker slot.

			npm.FetchBatch(
				results,
				errors,
				batch,
			)
		}(batch)
	}
}

func scheduleSearches(
	wg *sync.WaitGroup,
	queue chan struct{},
	results chan<- npm.SearchResponseObject,
	errors chan<- error,
	queries []string,
) {
	for _, query := range queries {
		wg.Add(1)
		go func(query string) {
			defer wg.Done()
			queue <- struct{}{}        // NOTE: Acquire worker slot.
			defer func() { <-queue }() // NOTE: Release worker slot.

			npm.Search(
				results,
				errors,
				query,
				0,
				1,
				0,
			)
		}(query)
	}
}
