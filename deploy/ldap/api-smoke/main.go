package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	ldapsmoke "vectis/deploy/ldap"
	ldapauth "vectis/extensions/auth/ldap"
)

func main() {
	var opts ldapsmoke.APISmokeOptions
	var outputJSON bool

	flag.StringVar(&opts.LDAP.URL, "url", envDefault(ldapauth.EnvURL, ldapauth.DefaultSmokeURL), "LDAP server URL")
	flag.StringVar(&opts.LDAP.BindDN, "bind-dn", os.Getenv(ldapauth.EnvBindDN), "LDAP service-account bind DN")
	flag.StringVar(&opts.LDAP.BindPassword, "bind-password", os.Getenv(ldapauth.EnvBindPassword), "LDAP service-account bind password; prefer --bind-password-file")
	flag.StringVar(&opts.LDAP.BindPasswordFile, "bind-password-file", os.Getenv(ldapauth.EnvBindPasswordFile), "File containing the LDAP service-account bind password")
	flag.StringVar(&opts.LDAP.BaseDN, "base-dn", envDefault(ldapauth.EnvBaseDN, ldapauth.DefaultSmokeBaseDN), "LDAP base DN used for user search")
	flag.StringVar(&opts.LDAP.UserFilter, "user-filter", envDefault(ldapauth.EnvUserFilter, "(uid={username})"), "LDAP user search filter")
	flag.StringVar(&opts.LDAP.SubjectAttribute, "subject-attribute", os.Getenv(ldapauth.EnvSubjectAttribute), "LDAP attribute used as the stable external subject")
	flag.StringVar(&opts.LDAP.UsernameAttribute, "username-attribute", envDefault(ldapauth.EnvUsernameAttribute, "uid"), "LDAP attribute mapped to username")
	flag.StringVar(&opts.LDAP.DisplayNameAttribute, "display-name-attribute", envDefault(ldapauth.EnvDisplayNameAttribute, "cn"), "LDAP attribute mapped to display name")
	flag.BoolVar(&opts.LDAP.StartTLS, "start-tls", envBoolDefault(ldapauth.EnvStartTLS, false), "Upgrade ldap:// connections with StartTLS")
	flag.StringVar(&opts.LDAP.Username, "username", ldapauth.DefaultSmokeUsername, "Username to authenticate through the API login path")
	flag.StringVar(&opts.LDAP.Password, "password", ldapauth.DefaultSmokePassword, "Password for the smoke username")
	flag.StringVar(&opts.LDAP.WrongPassword, "wrong-password", ldapauth.DefaultSmokeWrongPassword, "Password expected to fail through the API login path")
	flag.StringVar(&opts.BootstrapToken, "bootstrap-token", ldapsmoke.DefaultAPISmokeBootstrapToken, "API bootstrap token used for in-memory smoke setup")
	flag.StringVar(&opts.AdminUsername, "admin-username", ldapsmoke.DefaultAPISmokeAdminUsername, "API setup admin username")
	flag.StringVar(&opts.AdminPassword, "admin-password", ldapsmoke.DefaultAPISmokeAdminPassword, "API setup admin password")
	flag.DurationVar(&opts.Timeout, "timeout", ldapauth.DefaultSmokeTimeout, "Maximum time to wait for the LDAP API smoke")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.Stdout = os.Stdout
	if outputJSON {
		opts.Stdout = os.Stderr
	}

	result, err := ldapsmoke.RunAPISmoke(ctx, opts)
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

	fmt.Fprintf(os.Stdout, "LDAP API smoke succeeded: username=%s user_id=%d token_returned=%t api_probe=%s setup_external_identity_linked=%t password_login_denied=%t external_login_matched_setup=%t wrong_password_denied=%t\n", result.Username, result.UserID, result.TokenReturned, result.AuthenticatedAPIProbePath, result.SetupExternalIdentityLinked, result.PasswordLoginDenied, result.ExternalLoginMatchedSetup, result.WrongPasswordDenied)
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
