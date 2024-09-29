package http

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func FetchJSON[T any](req *http.Request, result *T) error {
	fmt.Printf("FETCH %s %s\n", req.Method, req.URL.String())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf(
			"failed to %s %s (%d)",
			req.Method,
			req.URL.String(),
			resp.StatusCode,
		)
	}

	decoder := json.NewDecoder(resp.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(result)
	if err != nil {
		return err
	}
	return nil
}
