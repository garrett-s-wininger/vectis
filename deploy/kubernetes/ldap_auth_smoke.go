package kubernetes

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	ldapsmoke "vectis/deploy/ldap"
	ldapauth "vectis/extensions/auth/ldap"
)

const (
	smokeLDAPDeploymentName    = "deployment/vectis-ldap"
	smokeLDAPServiceName       = "service/vectis-ldap"
	smokeLDAPConfigMapResource = "configmap/vectis-ldap-bootstrap"
	smokeLDAPContainerName     = "ldap"
	smokeLDAPRemotePort        = 389
	smokeAPIDeploymentName     = "deployment/vectis-api"
	smokeAPIContainerName      = "api"
	smokeLDAPOrganization      = "Vectis Smoke"
	smokeLDAPDomain            = "example.org"
	smokeLDAPAdminPassword     = "admin-secret"
	smokeLDAPBindUsername      = "vectis"
	smokeLDAPAPITimeout        = "30s"
)

func runLDAPAuthSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	if err := validateLDAPAuthSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	out := opts.Stdout
	fmt.Fprintf(out, "Applying LDAP auth smoke fixture %s\n", smokeLDAPDeploymentName)
	setupCtx, setupCancel := context.WithTimeout(ctx, opts.Wait)
	if err := applySmokeLDAPFixture(setupCtx, opts); err != nil {
		setupCancel()
		return SmokeResult{}, err
	}
	setupCancel()

	if !opts.LDAPKeepFixture {
		defer cleanupSmokeLDAPFixture(opts)
	}

	fmt.Fprintf(out, "Starting LDAP port-forward on 127.0.0.1:%d\n", opts.LDAPLocalPort)
	ldapPF, err := startSmokePortForward(ctx, opts, smokeLDAPServiceName, []smokePortMapping{{
		LocalPort:  opts.LDAPLocalPort,
		RemotePort: smokeLDAPRemotePort,
	}}, "LDAP")
	if err != nil {
		return SmokeResult{}, err
	}
	defer ldapPF.stop()

	localLDAPURL := fmt.Sprintf("ldap://127.0.0.1:%d", opts.LDAPLocalPort)
	checkCtx, checkCancel := context.WithTimeout(ctx, opts.Wait)
	if _, err := ldapauth.RunSmoke(checkCtx, smokeLDAPProviderOptions(opts, localLDAPURL)); err != nil {
		checkCancel()
		return SmokeResult{}, fmt.Errorf("validate LDAP fixture: %w", err)
	}
	checkCancel()

	apiEnv := smokeLDAPAPIEnv(opts)
	snapshotCtx, snapshotCancel := context.WithTimeout(ctx, opts.Wait)
	apiEnvSnapshot, err := snapshotSmokeWorkloadEnv(snapshotCtx, opts, smokeAPIDeploymentName, smokeAPIContainerName, mapKeys(apiEnv))
	snapshotCancel()
	if err != nil {
		return SmokeResult{}, err
	}
	defer restoreSmokeWorkloadEnv(opts, smokeAPIDeploymentName, smokeAPIContainerName, apiEnvSnapshot)

	patchCtx, patchCancel := context.WithTimeout(ctx, opts.Wait)
	if err := setSmokeWorkloadEnv(patchCtx, opts, smokeAPIDeploymentName, smokeAPIContainerName, apiEnv); err != nil {
		patchCancel()
		return SmokeResult{}, err
	}
	patchCancel()

	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	apiPF, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	defer apiPF.stop()

	apiURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	apiCtx, apiCancel := context.WithTimeout(ctx, opts.Wait)
	apiResult, err := ldapsmoke.RunAPISmoke(apiCtx, ldapsmoke.APISmokeOptions{
		APIURL:         apiURL,
		LDAP:           smokeLDAPProviderOptions(opts, localLDAPURL),
		BootstrapToken: opts.LDAPBootstrapToken,
		AdminUsername:  opts.LDAPAdminUsername,
		AdminPassword:  opts.LDAPAdminPassword,
		Timeout:        opts.Wait,
		Stdout:         out,
	})
	apiCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	result := SmokeResult{
		Status:               "ok",
		Context:              strings.TrimSpace(opts.Context),
		Namespace:            opts.Namespace,
		LDAPURL:              opts.LDAPClusterURL,
		LDAPUsername:         apiResult.Username,
		LDAPUserID:           apiResult.UserID,
		LDAPSetupPerformed:   apiResult.SetupPerformed,
		LDAPSetupPreexisting: apiResult.SetupAlreadyComplete,
	}

	fmt.Fprintf(out, "Kubernetes LDAP auth smoke succeeded: username=%s user_id=%d setup_performed=%t setup_preexisting=%t\n", apiResult.Username, apiResult.UserID, apiResult.SetupPerformed, apiResult.SetupAlreadyComplete)
	return result, nil
}

func validateLDAPAuthSmokeOptions(opts SmokeOptions) error {
	if opts.LDAPImage == "" {
		return fmt.Errorf("LDAP image is required")
	}

	if opts.LDAPLocalPort <= 0 || opts.LDAPLocalPort > 65535 {
		return fmt.Errorf("LDAP local port must be between 1 and 65535")
	}

	if opts.LDAPClusterURL == "" {
		return fmt.Errorf("LDAP cluster URL is required")
	}

	parsed, err := url.Parse(opts.LDAPClusterURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "ldap" && parsed.Scheme != "ldaps") {
		return fmt.Errorf("LDAP cluster URL must be an ldap:// or ldaps:// URL")
	}

	if opts.LDAPBootstrapLDIF == "" {
		return fmt.Errorf("LDAP bootstrap LDIF path is required")
	}

	if opts.LDAPBaseDN == "" {
		return fmt.Errorf("LDAP base DN is required")
	}

	if opts.LDAPBindDN == "" {
		return fmt.Errorf("LDAP bind DN is required")
	}

	if opts.LDAPBindPassword == "" {
		return fmt.Errorf("LDAP bind password is required")
	}

	if opts.LDAPUserFilter == "" {
		return fmt.Errorf("LDAP user filter is required")
	}

	if opts.LDAPUsername == "" {
		return fmt.Errorf("LDAP username is required")
	}

	if opts.LDAPPassword == "" {
		return fmt.Errorf("LDAP password is required")
	}

	if opts.LDAPBootstrapToken == "" {
		return fmt.Errorf("LDAP API bootstrap token is required")
	}

	if opts.LDAPAdminUsername == "" {
		return fmt.Errorf("LDAP setup admin username is required")
	}

	if opts.LDAPAdminPassword == "" {
		return fmt.Errorf("LDAP setup admin password is required")
	}

	return nil
}

func applySmokeLDAPFixture(ctx context.Context, opts SmokeOptions) error {
	ldif, err := os.ReadFile(opts.LDAPBootstrapLDIF)
	if err != nil {
		return fmt.Errorf("read LDAP bootstrap LDIF %s: %w", opts.LDAPBootstrapLDIF, err)
	}

	return applySmokeManifest(ctx, opts, "LDAP smoke fixture", smokeLDAPFixtureManifest(opts, string(ldif)), smokeLDAPDeploymentName)
}

func cleanupSmokeLDAPFixture(opts SmokeOptions) {
	cleanupSmokeResources(opts, "LDAP smoke fixture", smokeLDAPDeploymentName, smokeLDAPServiceName, smokeLDAPConfigMapResource)
}

func smokeLDAPFixtureManifest(opts SmokeOptions, ldif string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: vectis-ldap-bootstrap
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: ldap-auth-smoke
data:
  001-vectis-smoke.ldif: %s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vectis-ldap
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: ldap-auth-smoke
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: vectis
      app.kubernetes.io/component: ldap-auth-smoke
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vectis
        app.kubernetes.io/component: ldap-auth-smoke
    spec:
      automountServiceAccountToken: false
      containers:
        - name: %s
          image: %s
          imagePullPolicy: IfNotPresent
          args:
            - --copy-service
          env:
            - name: LDAP_ORGANISATION
              value: %s
            - name: LDAP_DOMAIN
              value: %s
            - name: LDAP_ADMIN_PASSWORD
              value: %s
            - name: LDAP_READONLY_USER
              value: "true"
            - name: LDAP_READONLY_USER_USERNAME
              value: %s
            - name: LDAP_READONLY_USER_PASSWORD
              value: %s
            - name: LDAP_TLS
              value: "false"
          ports:
            - name: ldap
              containerPort: %d
          volumeMounts:
            - name: bootstrap
              mountPath: /container/service/slapd/assets/config/bootstrap/ldif/custom/001-vectis-smoke.ldif
              subPath: 001-vectis-smoke.ldif
              readOnly: true
          readinessProbe:
            tcpSocket:
              port: %d
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            tcpSocket:
              port: %d
            initialDelaySeconds: 20
            periodSeconds: 10
      volumes:
        - name: bootstrap
          configMap:
            name: vectis-ldap-bootstrap
---
apiVersion: v1
kind: Service
metadata:
  name: vectis-ldap
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: ldap-auth-smoke
spec:
  selector:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: ldap-auth-smoke
  ports:
    - name: ldap
      port: %d
      targetPort: %d
`,
		yamlQuote(ldif),
		smokeLDAPContainerName,
		yamlQuote(opts.LDAPImage),
		yamlQuote(smokeLDAPOrganization),
		yamlQuote(smokeLDAPDomain),
		yamlQuote(smokeLDAPAdminPassword),
		yamlQuote(smokeLDAPBindUsername),
		yamlQuote(opts.LDAPBindPassword),
		smokeLDAPRemotePort,
		smokeLDAPRemotePort,
		smokeLDAPRemotePort,
		smokeLDAPRemotePort,
		smokeLDAPRemotePort,
	)
}

func smokeLDAPProviderOptions(opts SmokeOptions, url string) ldapauth.SmokeOptions {
	return ldapauth.SmokeOptions{
		URL:                  url,
		BindDN:               opts.LDAPBindDN,
		BindPassword:         opts.LDAPBindPassword,
		BaseDN:               opts.LDAPBaseDN,
		UserFilter:           opts.LDAPUserFilter,
		SubjectAttribute:     opts.LDAPSubjectAttr,
		UsernameAttribute:    opts.LDAPUsernameAttr,
		DisplayNameAttribute: opts.LDAPDisplayNameAttr,
		Username:             opts.LDAPUsername,
		Password:             opts.LDAPPassword,
		WrongPassword:        opts.LDAPWrongPassword,
		ExpectedSubject:      opts.LDAPExpectedSubject,
		ExpectedUsername:     opts.LDAPUsername,
		ExpectedDisplayName:  opts.LDAPExpectedName,
		Timeout:              opts.Wait,
		Stdout:               opts.Stdout,
	}
}

func smokeLDAPAPIEnv(opts SmokeOptions) map[string]string {
	return map[string]string{
		"VECTIS_API_AUTH_ENABLED":                     "true",
		"VECTIS_API_AUTHZ_ENGINE":                     "authenticated_full",
		"VECTIS_API_SESSION_ALLOW_INSECURE_COOKIES":   "true",
		"VECTIS_API_AUTH_LDAP_PROVIDER_ID":            ldapauth.DefaultProviderID,
		"VECTIS_API_AUTH_LDAP_URL":                    opts.LDAPClusterURL,
		"VECTIS_API_AUTH_LDAP_BIND_DN":                opts.LDAPBindDN,
		"VECTIS_API_AUTH_LDAP_BIND_PASSWORD":          opts.LDAPBindPassword,
		"VECTIS_API_AUTH_LDAP_BASE_DN":                opts.LDAPBaseDN,
		"VECTIS_API_AUTH_LDAP_USER_FILTER":            opts.LDAPUserFilter,
		"VECTIS_API_AUTH_LDAP_SUBJECT_ATTRIBUTE":      opts.LDAPSubjectAttr,
		"VECTIS_API_AUTH_LDAP_USERNAME_ATTRIBUTE":     opts.LDAPUsernameAttr,
		"VECTIS_API_AUTH_LDAP_DISPLAY_NAME_ATTRIBUTE": opts.LDAPDisplayNameAttr,
		"VECTIS_API_AUTH_LDAP_START_TLS":              "false",
		"VECTIS_API_AUTH_LDAP_TIMEOUT":                smokeLDAPAPITimeout,
		"VECTIS_API_AUTH_LDAP_AUTO_LINK_USERS":        "false",
		"VECTIS_API_AUTH_LDAP_AUTO_CREATE_USERS":      "false",
	}
}
