package npm

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	_http "github.com/make-github-pseudonymous-again/npm-downloads/internals/http"
)

const (
	NPM_REGISTRY_API                     = "https://registry.npmjs.org"
	NPM_REGISTRY_API_SEARCH_ENDPOINT     = "%s/-/v1/search"
	PageSize                         int = 250
)

type Package struct {
	Name        string              `json:"name"`
	Scope       string              `json:"scope"`
	Version     string              `json:"version"`
	Description string              `json:"description"`
	Keywords    []string            `json:"keywords"`
	Date        string              `json:"date"`
	Links       map[string]string   `json:"links"`
	Author      map[string]string   `json:"author"`
	Publisher   map[string]string   `json:"publisher"`
	Maintainers []map[string]string `json:"maintainers"`
}

type Flags struct {
	Insecure int `json:"insecure"`
}

type ScoreDetail struct {
	Quality     float64 `json:"quality"`
	Popularity  float64 `json:"popularity"`
	Maintenance float64 `json:"maintenance"`
}

type Score struct {
	Final  float64     `json:"final"`
	Detail ScoreDetail `json:"detail"`
}

type SearchResponseObject struct {
	Package     Package `json:"package"`
	Flags       Flags   `json:"flags"`
	Score       Score   `json:"score"`
	SearchScore float64 `json:"searchScore"`
}

type SearchResponse struct {
	Objects []SearchResponseObject `json:"objects"`
	Total   int                    `json:"total"`
	Time    string                 `json:"time"`
}

func _encodeFloat64(x float64) string {
	return strconv.FormatFloat(x, 'f', -1, 64)
}

func _encodeInt(x int) string {
	return strconv.FormatInt(int64(x), 10)
}

func _search_req(
	text string,
	size int,
	from int,
	quality float64,
	popularity float64,
	maintenance float64,
) *http.Request {
	url := fmt.Sprintf(
		NPM_REGISTRY_API_SEARCH_ENDPOINT,
		NPM_REGISTRY_API,
	)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}

	q := req.URL.Query()
	q.Add("text", text)
	q.Add("size", _encodeInt(size))
	q.Add("from", _encodeInt(from))
	q.Add("quality", _encodeFloat64(quality))
	q.Add("popularity", _encodeFloat64(popularity))
	q.Add("maintenance", _encodeFloat64(maintenance))

	req.URL.RawQuery = q.Encode()

	return req
}

func Search(
	results chan<- SearchResponseObject,
	errors chan<- error,
	text string,
	quality float64,
	popularity float64,
	maintenance float64,
) {
	var offset int = 0
	step := PageSize
	for {
		req := _search_req(text, step, offset, quality, popularity, maintenance)
		var response SearchResponse
		err := _http.FetchJSON(req, &response)
		if err != nil {
			errors <- err
			log.Printf("%v\n", err)
			return
		}

		for _, object := range response.Objects {
			log.Printf("%v\n", object.Package.Name)
			results <- object
		}

		if len(response.Objects) < step {
			log.Printf("BREAK\n")
			break
		}

		offset += step
	}
}
