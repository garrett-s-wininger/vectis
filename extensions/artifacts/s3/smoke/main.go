package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	s3artifact "vectis/extensions/artifacts/s3"
)

func main() {
	var opts s3artifact.SmokeOptions
	var outputJSON bool

	flag.StringVar(&opts.Endpoint, "endpoint", os.Getenv(s3artifact.EnvEndpoint), "S3-compatible endpoint")
	flag.StringVar(&opts.Region, "region", envDefault(s3artifact.EnvRegion, s3artifact.DefaultSmokeRegion), "S3 signing region")
	flag.StringVar(&opts.Bucket, "bucket", envDefault(s3artifact.EnvBucket, s3artifact.DefaultSmokeBucket), "S3 bucket")
	flag.StringVar(&opts.Prefix, "prefix", os.Getenv(s3artifact.EnvPrefix), "S3 object key prefix")
	flag.StringVar(&opts.AccessKeyID, "access-key-id", os.Getenv(s3artifact.EnvAccessKeyID), "S3 access key id")
	flag.StringVar(&opts.SecretAccessKey, "secret-access-key", os.Getenv(s3artifact.EnvSecretAccessKey), "S3 secret access key; prefer --secret-access-key-file")
	flag.StringVar(&opts.SecretAccessKeyFile, "secret-access-key-file", os.Getenv(s3artifact.EnvSecretAccessKeyFile), "File containing the S3 secret access key")
	flag.StringVar(&opts.SessionToken, "session-token", os.Getenv(s3artifact.EnvSessionToken), "S3 session token")
	flag.BoolVar(&opts.PathStyle, "path-style", envBoolDefault(s3artifact.EnvPathStyle, true), "Use path-style S3 URLs")
	flag.BoolVar(&opts.CreateBucket, "create-bucket", s3artifact.DefaultSmokeCreateBucket, "Create the bucket before exercising blobs")
	flag.StringVar(&opts.Payload, "payload", s3artifact.DefaultSmokePayload, "Payload written and read by the smoke")
	flag.DurationVar(&opts.Timeout, "timeout", s3artifact.DefaultSmokeTimeout, "Maximum time to wait for the S3 smoke")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.Stdout = os.Stdout
	if outputJSON {
		opts.Stdout = os.Stderr
	}

	result, err := s3artifact.RunSmoke(ctx, opts)
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

	fmt.Fprintf(os.Stdout, "S3 artifact smoke succeeded: bucket=%s prefix=%s key=%s bytes=%d\n", result.Bucket, result.Prefix, result.BlobKey, result.Bytes)
}

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
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
