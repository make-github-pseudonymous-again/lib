package npm

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/make-github-pseudonymous-again/npm-downloads/internals/http"
)

const (
	NPM_DOWNLOADS_API                = "https://api.npmjs.org"
	NPM_DOWNLOADS_API_RANGE_ENDPOINT = "%s/downloads/range/%s/%s"
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

func FetchBatch(wg *sync.WaitGroup, resultsChan chan<- SinglePackageResponse, errorsChan chan<- error, batch Batch) {
	defer wg.Done()

	if len(batch.Packages) == 1 {
		FetchBatchSingle(
			resultsChan,
			errorsChan,
			batch,
		)
	}
	if len(batch.Packages) >= 2 {
		FetchBatchMany(
			resultsChan,
			errorsChan,
			batch,
		)
	}

}

func FetchBatchSingle(resultsChan chan<- SinglePackageResponse, errorsChan chan<- error, batch Batch) {
	if len(batch.Packages) != 1 {
		panic("FetchBatchSingle can only handles batches of size == 1")
	}

	var response SinglePackageResponse
	err := http.FetchJSON(url(batch), &response)

	if err != nil {
		errorsChan <- err
		return
	}

	if response.Error != "" {
		errorsChan <- errors.New(response.Error)
	} else {
		resultsChan <- response
	}
}

func FetchBatchMany(resultsChan chan<- SinglePackageResponse, errorsChan chan<- error, batch Batch) {
	if len(batch.Packages) < 2 {
		panic("FetchBatchMany can only handles batches of size >= 1")
	}

	var responses MultiPackageResponse
	err := http.FetchJSON(url(batch), &responses)

	if err != nil {
		errorsChan <- err
		return
	}

	for key, response := range responses {
		if response.Package == "" {
			errorsChan <- fmt.Errorf("package %v not found", key)
		} else {
			resultsChan <- response
		}
	}
}
