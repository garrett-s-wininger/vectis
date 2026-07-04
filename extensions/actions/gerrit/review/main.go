package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	gerritaction "vectis/extensions/actions/gerrit"
)

func main() {
	if err := run(context.Background(), os.Getenv, os.ReadFile); err != nil {
		fmt.Fprintf(os.Stderr, "gerrit review action: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string, readFile func(string) ([]byte, error)) error {
	req, err := requestFromEnv(getenv, readFile)
	if err != nil {
		return err
	}

	client := gerritaction.Client{
		BaseURL:  req.baseURL,
		Username: req.username,
		Password: req.password,
	}

	if err := client.PostReview(ctx, gerritaction.ReviewRequest{
		Change:    req.change,
		Revision:  req.revision,
		Message:   req.message,
		Label:     req.label,
		LabelVote: req.labelVote,
		Tag:       req.tag,
	}); err != nil {
		return err
	}

	fmt.Printf("Gerrit review posted successfully: change=%s revision=%s\n", req.change, req.revision)
	return nil
}

type reviewRequest struct {
	baseURL   string
	change    string
	revision  string
	message   string
	label     string
	labelVote int
	tag       string
	username  string
	password  string
}

func requestFromEnv(getenv func(string) string, readFile func(string) ([]byte, error)) (reviewRequest, error) {
	baseURL, err := requiredEnv(getenv, "VECTIS_INPUT_URL")
	if err != nil {
		return reviewRequest{}, err
	}

	if hasCredentialedHTTPURL(baseURL) {
		return reviewRequest{}, fmt.Errorf("url must not include embedded credentials")
	}

	if !gerritaction.IsHTTPURL(baseURL) {
		return reviewRequest{}, fmt.Errorf("url must use http or https")
	}

	change, err := requiredSingleLineEnv(getenv, "VECTIS_INPUT_CHANGE")
	if err != nil {
		return reviewRequest{}, err
	}

	revision, err := optionalSingleLineEnv(getenv, "VECTIS_INPUT_REVISION", gerritaction.DefaultReviewRevision)
	if err != nil {
		return reviewRequest{}, err
	}

	message, err := requiredEnv(getenv, "VECTIS_INPUT_MESSAGE")
	if err != nil {
		return reviewRequest{}, err
	}

	label, err := optionalSingleLineEnv(getenv, "VECTIS_INPUT_LABEL", "")
	if err != nil {
		return reviewRequest{}, err
	}

	var labelVote int
	value := strings.TrimSpace(getenv("VECTIS_INPUT_VALUE"))
	switch {
	case label == "" && value != "":
		return reviewRequest{}, fmt.Errorf("value requires label")
	case label != "" && value == "":
		return reviewRequest{}, fmt.Errorf("value is required when label is set")
	case label != "":
		vote, err := gerritaction.ParseLabelValue(value)
		if err != nil {
			return reviewRequest{}, err
		}

		labelVote = vote
	}

	tag, err := optionalSingleLineEnv(getenv, "VECTIS_INPUT_TAG", gerritaction.DefaultReviewTag)
	if err != nil {
		return reviewRequest{}, err
	}

	username, err := requiredSingleLineEnv(getenv, "VECTIS_INPUT_USERNAME")
	if err != nil {
		return reviewRequest{}, err
	}

	passwordFile, err := requiredSingleLineEnv(getenv, "VECTIS_INPUT_PASSWORD_FILE")
	if err != nil {
		return reviewRequest{}, err
	}

	passwordPath, err := workspaceRelativePath(getenv("VECTIS_WORKSPACE"), passwordFile)
	if err != nil {
		return reviewRequest{}, fmt.Errorf("password_file: %w", err)
	}

	passwordBytes, err := readFile(passwordPath)
	if err != nil {
		return reviewRequest{}, fmt.Errorf("read password_file: %w", err)
	}

	password := strings.TrimRight(string(passwordBytes), "\r\n")
	if password == "" {
		return reviewRequest{}, fmt.Errorf("password_file must not be empty")
	}

	return reviewRequest{
		baseURL:   baseURL,
		change:    change,
		revision:  revision,
		message:   message,
		label:     label,
		labelVote: labelVote,
		tag:       tag,
		username:  username,
		password:  password,
	}, nil
}

func requiredEnv(getenv func(string) string, key string) (string, error) {
	value := getenv(key)
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required", key)
	}

	return value, nil
}

func requiredSingleLineEnv(getenv func(string) string, key string) (string, error) {
	value, err := requiredEnv(getenv, key)
	if err != nil {
		return "", err
	}

	value = strings.TrimSpace(value)
	if containsLineBreak(value) {
		return "", fmt.Errorf("%s must be a single line", key)
	}

	return value, nil
}

func optionalSingleLineEnv(getenv func(string) string, key, fallback string) (string, error) {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback, nil
	}

	if containsLineBreak(value) {
		return "", fmt.Errorf("%s must be a single line", key)
	}

	return value, nil
}

func workspaceRelativePath(workspace, rawPath string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", fmt.Errorf("is required")
	}

	if filepath.IsAbs(rawPath) {
		return "", fmt.Errorf("must be relative to the workspace")
	}

	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return "", fmt.Errorf("workspace is required")
	}

	root, err := filepath.Abs(workspace)
	if err != nil {
		return "", fmt.Errorf("resolve workspace: %w", err)
	}

	fullPath, err := filepath.Abs(filepath.Join(root, filepath.Clean(rawPath)))
	if err != nil {
		return "", fmt.Errorf("resolve path: %w", err)
	}

	rel, err := filepath.Rel(root, fullPath)
	if err != nil {
		return "", fmt.Errorf("resolve path relative to workspace: %w", err)
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("must stay inside the workspace")
	}

	return fullPath, nil
}

func hasCredentialedHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return false
	}

	return u.Scheme == "http" || u.Scheme == "https"
}

func containsLineBreak(value string) bool {
	return strings.ContainsAny(value, "\x00\r\n")
}
