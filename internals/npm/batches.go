package npm

import (
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
	Start     string          `json:"start"`
	End       string          `json:"end"`
	Package   string          `json:"package"`
	Downloads []DailyDownload `json:"downloads"`
}

// Struct for a multi-package response
type MultiPackageResponse map[string]SinglePackageResponse

func FetchBatch(wg *sync.WaitGroup, resultsChan chan<- SinglePackageResponse, errorsChan chan<- error, period string, batch []string) {
	defer wg.Done()
	namesJoined := strings.Join(batch, ",")

	url := fmt.Sprintf(
		NPM_DOWNLOADS_API_RANGE_ENDPOINT,
		NPM_DOWNLOADS_API,
		period,
		namesJoined,
	)

	if len(batch) == 1 {
		FetchBatchSingle(
			resultsChan,
			errorsChan,
			url,
		)
	}
	if len(batch) >= 2 {
		FetchBatchMany(
			resultsChan,
			errorsChan,
			url,
		)
	}

}

func FetchBatchSingle(resultsChan chan<- SinglePackageResponse, errorsChan chan<- error, url string) {
	var object SinglePackageResponse
	err := http.FetchJSON(url, &object)

	if err != nil {
		errorsChan <- err
		return
	}

	resultsChan <- object
}

func FetchBatchMany(resultsChan chan<- SinglePackageResponse, errorsChan chan<- error, url string) {
	var object MultiPackageResponse
	err := http.FetchJSON(url, &object)

	if err != nil {
		errorsChan <- err
		return
	}

	for _, singleResp := range object {
		resultsChan <- singleResp
	}
}
