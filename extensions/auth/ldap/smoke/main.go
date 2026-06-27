package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	ldapauth "vectis/extensions/auth/ldap"
)

func main() {
	var opts ldapauth.SmokeOptions
	var outputJSON bool

	flag.StringVar(&opts.URL, "url", envDefault(ldapauth.EnvURL, ldapauth.DefaultSmokeURL), "LDAP server URL")
	flag.StringVar(&opts.BindDN, "bind-dn", os.Getenv(ldapauth.EnvBindDN), "LDAP service-account bind DN")
	flag.StringVar(&opts.BindPassword, "bind-password", os.Getenv(ldapauth.EnvBindPassword), "LDAP service-account bind password; prefer --bind-password-file")
	flag.StringVar(&opts.BindPasswordFile, "bind-password-file", os.Getenv(ldapauth.EnvBindPasswordFile), "File containing the LDAP service-account bind password")
	flag.StringVar(&opts.BaseDN, "base-dn", envDefault(ldapauth.EnvBaseDN, ldapauth.DefaultSmokeBaseDN), "LDAP base DN used for user search")
	flag.StringVar(&opts.UserFilter, "user-filter", envDefault(ldapauth.EnvUserFilter, "(uid={username})"), "LDAP user search filter")
	flag.StringVar(&opts.UsernameAttribute, "username-attribute", envDefault(ldapauth.EnvUsernameAttribute, "uid"), "LDAP attribute mapped to username")
	flag.StringVar(&opts.DisplayNameAttribute, "display-name-attribute", envDefault(ldapauth.EnvDisplayNameAttribute, "cn"), "LDAP attribute mapped to display name")
	flag.BoolVar(&opts.StartTLS, "start-tls", envBoolDefault(ldapauth.EnvStartTLS, false), "Upgrade ldap:// connections with StartTLS")
	flag.StringVar(&opts.Username, "username", ldapauth.DefaultSmokeUsername, "Username to authenticate")
	flag.StringVar(&opts.Password, "password", ldapauth.DefaultSmokePassword, "Password for the smoke username")
	flag.StringVar(&opts.WrongPassword, "wrong-password", ldapauth.DefaultSmokeWrongPassword, "Password expected to fail for the smoke username")
	flag.StringVar(&opts.ExpectedSubject, "expect-subject", "", "Expected LDAP subject DN")
	flag.StringVar(&opts.ExpectedUsername, "expect-username", "", "Expected mapped username")
	flag.StringVar(&opts.ExpectedDisplayName, "expect-display-name", "", "Expected mapped display name")
	flag.DurationVar(&opts.Timeout, "timeout", ldapauth.DefaultSmokeTimeout, "Maximum time to wait for the LDAP smoke")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.Stdout = os.Stdout
	if outputJSON {
		opts.Stdout = os.Stderr
	}

	result, err := ldapauth.RunSmoke(ctx, opts)
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

	fmt.Fprintf(os.Stdout, "LDAP smoke succeeded: url=%s username=%s subject=%s wrong_password_denied=%t\n", result.URL, result.Username, result.Subject, result.WrongPasswordDenied)
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
