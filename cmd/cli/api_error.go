package main

import (
	"encoding/json"
	"io"
	"net/http"
)

type cliAPIError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Error   string         `json:"error"`
	Detail  string         `json:"detail"`
	Details map[string]any `json:"details"`
}

func readCLIAPIError(resp *http.Response) (cliAPIError, bool) {
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil || len(body) == 0 {
		return cliAPIError{}, false
	}

	var apiErr cliAPIError
	if err := json.Unmarshal(body, &apiErr); err != nil {
		return cliAPIError{}, false
	}

	return apiErr, apiErr.Code != "" || apiErr.Message != "" || apiErr.Error != "" || apiErr.Detail != "" || len(apiErr.Details) > 0
}
