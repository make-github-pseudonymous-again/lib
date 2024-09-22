package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func FetchJSON[T any](url string, result *T) error {
	fmt.Printf("FETCH %s\n", url)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, result)
	if err != nil {
		return fmt.Errorf("failed to parse response: %s", body)
	}
	return nil
}
