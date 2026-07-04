package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	knoxsecrets "vectis/extensions/secrets/knox"
)

func main() {
	var opts knoxsecrets.SmokeOptions
	var outputJSON bool

	flag.StringVar(&opts.URL, "url", os.Getenv(knoxsecrets.EnvURL), "Knox base URL")
	flag.StringVar(&opts.AuthToken, "auth-token", os.Getenv(knoxsecrets.EnvAuthToken), "Knox Authorization header value; prefer --auth-token-file")
	flag.StringVar(&opts.AuthTokenFile, "auth-token-file", os.Getenv(knoxsecrets.EnvAuthTokenFile), "File containing the Knox Authorization header value")
	flag.BoolVar(&opts.InsecureSkipVerify, "insecure-skip-verify", envBoolDefault(knoxsecrets.EnvInsecureSkipVerify, false), "Skip Knox TLS certificate verification")
	flag.StringVar(&opts.CAFile, "ca-file", os.Getenv(knoxsecrets.EnvCAFile), "Knox server CA certificate file")
	flag.StringVar(&opts.ClientCertFile, "client-cert-file", os.Getenv(knoxsecrets.EnvClientCertFile), "Knox mTLS client certificate file")
	flag.StringVar(&opts.ClientKeyFile, "client-key-file", os.Getenv(knoxsecrets.EnvClientKeyFile), "Knox mTLS client private key file")
	flag.StringVar(&opts.Ref, "ref", knoxsecrets.DefaultSmokeRef, "Knox secret ref")
	flag.StringVar(&opts.ID, "id", knoxsecrets.DefaultSmokeID, "Expected delivered secret id")
	flag.StringVar(&opts.Path, "path", knoxsecrets.DefaultSmokePath, "Expected delivered secret path")
	flag.StringVar(&opts.ExpectedData, "expected-data", "", "Expected secret data; prefer --expected-sha256")
	flag.StringVar(&opts.ExpectedSHA256, "expected-sha256", "", "Expected secret SHA-256")
	flag.StringVar(&opts.WrongAuthToken, "wrong-auth-token", "", "Optional token expected to be denied")
	flag.StringVar(&opts.MissingRef, "missing-ref", "", "Optional Knox ref expected to be missing")
	flag.DurationVar(&opts.Timeout, "timeout", knoxsecrets.DefaultSmokeTimeout, "Maximum time to wait for the Knox smoke")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.Stdout = os.Stdout
	if outputJSON {
		opts.Stdout = os.Stderr
	}

	result, err := knoxsecrets.RunSmoke(ctx, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: write JSON result: %v\n", err)
			os.Exit(1)
		}

		return
	}

	fmt.Fprintf(os.Stdout, "Knox smoke succeeded: ref=%s id=%s path=%s bytes=%d sha256=%s wrong_token_denied=%t missing_ref_denied=%t\n", result.Ref, result.ID, result.Path, result.Bytes, result.SHA256, result.WrongTokenDenied, result.MissingRefDenied)
}

func envBoolDefault(key string, fallback bool) bool {
	value := os.Getenv(key)
	switch value {
	case "1", "t", "T", "true", "TRUE", "True", "yes", "YES", "Yes", "y", "Y", "on", "ON", "On":
		return true
	case "0", "f", "F", "false", "FALSE", "False", "no", "NO", "No", "n", "N", "off", "OFF", "Off":
		return false
	default:
		return fallback
	}
}
