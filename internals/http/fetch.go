package http

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func FetchJSON[T any](req *http.Request, result *T) error {
	fmt.Printf("FETCH %s\n", req.URL.String())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("failed to fetch %s: received status code %d", req.URL.String(), resp.StatusCode)
	}

	fmt.Printf("JSON %s\n", req.URL.String())
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(result)
	if err != nil {
		return fmt.Errorf("failed to parse response")
	}
	fmt.Printf("DONE %s\n", req.URL.String())
	return nil
}
