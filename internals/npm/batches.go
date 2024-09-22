package npm

import (
	"fmt"
	"strings"
	"sync"

	"github.com/make-github-pseudonymous-again/npm-downloads/internals/arrays"
	"github.com/make-github-pseudonymous-again/npm-downloads/internals/http"
	"github.com/make-github-pseudonymous-again/npm-downloads/internals/npm/names"
)

const (
	NPM_DOWNLOADS_API                = "https://api.npmjs.org"
	NPM_DOWNLOADS_API_RANGE_ENDPOINT = "%s/downloads/range/%s/%s"
	MaxBatchSize                     = 128
)

// Struct for daily downloads
type DailyDownload struct {
	Downloads int    `json:"downloads"`
	Day       string `json:"day"`
}

// Struct for a single-package response
type SinglePackageResponse struct {
	Error     string          `json:"error"`
	Start     string          `json:"start"`
	End       string          `json:"end"`
	Package   string          `json:"package"`
	Downloads []DailyDownload `json:"downloads"`
}

// Struct for a multi-package response
type MultiPackageResponse map[string]SinglePackageResponse

type Batch struct {
	Period   string
	Packages []string
}

func url(batch Batch) string {
	namesJoined := strings.Join(batch.Packages, ",")
	return fmt.Sprintf(
		NPM_DOWNLOADS_API_RANGE_ENDPOINT,
		NPM_DOWNLOADS_API,
		batch.Period,
		namesJoined,
	)
}

func FetchBatch(wg *sync.WaitGroup, results chan<- SinglePackageResponse, errors chan<- error, batch Batch) {
	defer wg.Done()

	if len(batch.Packages) == 1 {
		FetchBatchSingle(
			results,
			errors,
			batch,
		)
	}

	if len(batch.Packages) >= 2 {
		FetchBatchMany(
			results,
			errors,
			batch,
		)
	}
}

func FetchBatchSingle(results chan<- SinglePackageResponse, errors chan<- error, batch Batch) {
	if len(batch.Packages) != 1 {
		panic("FetchBatchSingle can only handles batches of size == 1")
	}

	var response SinglePackageResponse
	err := http.FetchJSON(url(batch), &response)

	if err != nil {
		errors <- err
		return
	}

	if response.Error != "" {
		errors <- fmt.Errorf("%s", response.Error)
	} else {
		results <- response
	}
}

func FetchBatchMany(results chan<- SinglePackageResponse, errors chan<- error, batch Batch) {
	if len(batch.Packages) < 2 {
		panic("FetchBatchMany can only handles batches of size >= 1")
	}

	var responses MultiPackageResponse
	err := http.FetchJSON(url(batch), &responses)

	if err != nil {
		errors <- err
		return
	}

	for key, response := range responses {
		if response.Package == "" {
			errors <- fmt.Errorf("package %v not found", key)
		} else {
			results <- response
		}
	}
}

func PackageDownloadBatches(period string, packageNames []string) []Batch {
	// NOTE: Partition between scoped and non-scoped packages.
	var scopedPackages []string
	var nonScopedPackages []string

	for _, pkg := range packageNames {
		if names.IsScopedPackageName(pkg) {
			scopedPackages = append(scopedPackages, pkg)
		} else {
			nonScopedPackages = append(nonScopedPackages, pkg)
		}
	}

	// NOTE: Group non-scoped packages into batches.
	nonScopedBatches := arrays.Chunk(nonScopedPackages, MaxBatchSize)

	// NOTE: Return all batches.
	var batches []Batch

	for _, packages := range nonScopedBatches {
		batch := Batch{
			Period:   period,
			Packages: packages,
		}
		batches = append(batches, batch)

	}

	for _, pkg := range scopedPackages {
		packages := []string{pkg}
		batch := Batch{
			Period:   period,
			Packages: packages,
		}
		batches = append(batches, batch)
	}

	return batches
}
