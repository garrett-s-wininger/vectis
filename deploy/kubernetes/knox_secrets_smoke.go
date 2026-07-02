package kubernetes

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	knoxsecrets "vectis/extensions/secrets/knox"
)

const (
	smokeKnoxDeploymentName       = "deployment/vectis-knox"
	smokeKnoxServiceName          = "service/vectis-knox"
	smokeKnoxServiceDNSName       = "vectis-knox"
	smokeKnoxCertSecretName       = "vectis-knox-smoke-certs"
	smokeKnoxCertSecretResource   = "secret/" + smokeKnoxCertSecretName
	smokeKnoxCertVolumeName       = "knox-smoke-certs"
	smokeKnoxContainerName        = "knox"
	smokeKnoxRemotePort           = 9000
	smokeSecretsStatefulSetName   = "statefulset/vectis-secrets"
	smokeSecretsContainerName     = "secrets"
	smokeKnoxCertificateTTL       = 24 * time.Hour
	smokeKnoxPolicyAllowEncrypted = "namespace=*;job=*;task=*;ref=encryptedfs://*"
	smokeKnoxPolicyAllowKnox      = "namespace=*;job=*;task=*;ref=knox://*"
)

type smokeKnoxCertBundle struct {
	CA         []byte
	ServerCert []byte
	ServerKey  []byte
	ClientCert []byte
	ClientKey  []byte
}

func runKnoxSecretsSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	if err := validateKnoxSecretsSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	out := opts.Stdout
	certs, err := newSmokeKnoxCertBundle(opts)
	if err != nil {
		return SmokeResult{}, err
	}

	certDir, cleanupCertDir, err := writeSmokeKnoxCertFiles(certs)
	if err != nil {
		return SmokeResult{}, err
	}
	defer cleanupCertDir()

	fmt.Fprintf(out, "Applying Knox secrets smoke fixture %s\n", smokeKnoxDeploymentName)
	setupCtx, setupCancel := context.WithTimeout(ctx, opts.Wait)
	if err := applySmokeKnoxFixture(setupCtx, opts, certs); err != nil {
		setupCancel()
		return SmokeResult{}, err
	}
	setupCancel()

	if !opts.KnoxKeepFixture {
		defer cleanupSmokeKnoxFixture(opts)
	}

	fmt.Fprintf(out, "Starting Knox port-forward on 127.0.0.1:%d\n", opts.KnoxLocalPort)
	knoxPF, err := startSmokePortForward(ctx, opts, smokeKnoxServiceName, []smokePortMapping{{
		LocalPort:  opts.KnoxLocalPort,
		RemotePort: smokeKnoxRemotePort,
	}}, "Knox")

	if err != nil {
		return SmokeResult{}, err
	}
	defer knoxPF.stop()

	localEndpoint := fmt.Sprintf("https://127.0.0.1:%d", opts.KnoxLocalPort)
	checkCtx, checkCancel := context.WithTimeout(ctx, opts.Wait)
	if _, err := knoxsecrets.RunSmoke(checkCtx, knoxsecrets.SmokeOptions{
		URL:            localEndpoint,
		AuthToken:      opts.KnoxAuthToken,
		CAFile:         filepath.Join(certDir, "ca.crt"),
		ClientCertFile: filepath.Join(certDir, "client.crt"),
		ClientKeyFile:  filepath.Join(certDir, "client.key"),
		Ref:            opts.KnoxRef,
		ID:             knoxsecrets.DefaultSmokeID,
		Path:           knoxsecrets.DefaultSmokePath,
		ExpectedData:   opts.KnoxExpectedData,
		WrongAuthToken: opts.KnoxWrongAuthToken,
		MissingRef:     opts.KnoxMissingRef,
		Timeout:        opts.Wait,
		Stdout:         out,
	}); err != nil {
		checkCancel()
		return SmokeResult{}, fmt.Errorf("validate Knox fixture: %w", err)
	}
	checkCancel()

	volumeCtx, volumeCancel := context.WithTimeout(ctx, opts.Wait)
	if err := patchSmokeSecretsKnoxCertVolume(volumeCtx, opts); err != nil {
		volumeCancel()
		return SmokeResult{}, err
	}
	volumeCancel()
	defer removeSmokeSecretsKnoxCertVolume(opts)

	secretsEnv := smokeKnoxSecretsEnv(opts)
	snapshotCtx, snapshotCancel := context.WithTimeout(ctx, opts.Wait)
	secretsEnvSnapshot, err := snapshotSmokeWorkloadEnv(snapshotCtx, opts, smokeSecretsStatefulSetName, smokeSecretsContainerName, mapKeys(secretsEnv))
	snapshotCancel()
	if err != nil {
		return SmokeResult{}, err
	}
	defer restoreSmokeWorkloadEnv(opts, smokeSecretsStatefulSetName, smokeSecretsContainerName, secretsEnvSnapshot)

	patchCtx, patchCancel := context.WithTimeout(ctx, opts.Wait)
	if err := setSmokeWorkloadEnv(patchCtx, opts, smokeSecretsStatefulSetName, smokeSecretsContainerName, secretsEnv); err != nil {
		patchCancel()
		return SmokeResult{}, err
	}
	patchCancel()

	result, err := runCanonicalSmoke(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}

	result.KnoxURL = opts.KnoxClusterURL
	result.KnoxRef = opts.KnoxRef
	fmt.Fprintf(out, "Kubernetes Knox secrets smoke succeeded: run_id=%s ref=%s\n", result.RunID, opts.KnoxRef)
	return result, nil
}

func validateKnoxSecretsSmokeOptions(opts SmokeOptions) error {
	if opts.KnoxImage == "" {
		return fmt.Errorf("Knox image is required")
	}

	if opts.KnoxLocalPort <= 0 || opts.KnoxLocalPort > 65535 {
		return fmt.Errorf("Knox local port must be between 1 and 65535")
	}

	if opts.KnoxClusterURL == "" {
		return fmt.Errorf("Knox cluster URL is required")
	}

	parsed, err := url.Parse(opts.KnoxClusterURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("Knox cluster URL must be an https URL")
	}

	if opts.KnoxCertMountPath == "" {
		return fmt.Errorf("Knox cert mount path is required")
	}

	if _, err := smokeKnoxPrincipalID(opts.KnoxAuthToken); err != nil {
		return err
	}

	if opts.KnoxKeyID == "" {
		return fmt.Errorf("Knox key id is required")
	}

	if opts.KnoxRef == "" {
		return fmt.Errorf("Knox ref is required")
	}

	if opts.KnoxExpectedData == "" {
		return fmt.Errorf("Knox expected data is required")
	}

	return nil
}

func applySmokeKnoxFixture(ctx context.Context, opts SmokeOptions, certs smokeKnoxCertBundle) error {
	return applySmokeManifest(ctx, opts, "Knox smoke fixture", smokeKnoxFixtureManifest(opts, certs), smokeKnoxDeploymentName)
}

func cleanupSmokeKnoxFixture(opts SmokeOptions) {
	cleanupSmokeResources(opts, "Knox smoke fixture", smokeKnoxDeploymentName, smokeKnoxServiceName, smokeKnoxCertSecretResource)
}

func smokeKnoxFixtureManifest(opts SmokeOptions, certs smokeKnoxCertBundle) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: knox-secrets-smoke
type: Opaque
data:
  ca.crt: %s
  server.crt: %s
  server.key: %s
  client.crt: %s
  client.key: %s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vectis-knox
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: knox-secrets-smoke
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: vectis
      app.kubernetes.io/component: knox-secrets-smoke
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vectis
        app.kubernetes.io/component: knox-secrets-smoke
    spec:
      automountServiceAccountToken: false
      containers:
        - name: %s
          image: %s
          imagePullPolicy: IfNotPresent
          env:
            - name: KNOX_SMOKE_ADDR
              value: ":9000"
            - name: KNOX_SMOKE_CERT_DIR
              value: /certs
            - name: KNOX_SMOKE_KEY_ID
              value: %s
            - name: KNOX_SMOKE_SECRET
              value: %s
            - name: KNOX_SMOKE_AUTH_TOKEN
              value: %s
          ports:
            - name: https
              containerPort: %d
          volumeMounts:
            - name: knox-certs
              mountPath: /certs
              readOnly: true
          readinessProbe:
            tcpSocket:
              port: %d
            initialDelaySeconds: 3
            periodSeconds: 5
          livenessProbe:
            tcpSocket:
              port: %d
            initialDelaySeconds: 10
            periodSeconds: 10
      volumes:
        - name: knox-certs
          secret:
            secretName: %s
---
apiVersion: v1
kind: Service
metadata:
  name: vectis-knox
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: knox-secrets-smoke
spec:
  selector:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: knox-secrets-smoke
  ports:
    - name: https
      port: %d
      targetPort: %d
`,
		yamlQuote(smokeKnoxCertSecretName),
		yamlQuote(base64.StdEncoding.EncodeToString(certs.CA)),
		yamlQuote(base64.StdEncoding.EncodeToString(certs.ServerCert)),
		yamlQuote(base64.StdEncoding.EncodeToString(certs.ServerKey)),
		yamlQuote(base64.StdEncoding.EncodeToString(certs.ClientCert)),
		yamlQuote(base64.StdEncoding.EncodeToString(certs.ClientKey)),
		smokeKnoxContainerName,
		yamlQuote(opts.KnoxImage),
		yamlQuote(opts.KnoxKeyID),
		yamlQuote(opts.KnoxExpectedData),
		yamlQuote(opts.KnoxAuthToken),
		smokeKnoxRemotePort,
		smokeKnoxRemotePort,
		smokeKnoxRemotePort,
		yamlQuote(smokeKnoxCertSecretName),
		smokeKnoxRemotePort,
		smokeKnoxRemotePort,
	)
}

func smokeKnoxSecretsEnv(opts SmokeOptions) map[string]string {
	return map[string]string{
		"VECTIS_SECRETS_PROVIDERS_KNOX_URL":              opts.KnoxClusterURL,
		"VECTIS_SECRETS_PROVIDERS_KNOX_AUTH_TOKEN":       opts.KnoxAuthToken,
		"VECTIS_SECRETS_PROVIDERS_KNOX_CA_FILE":          opts.KnoxCertMountPath + "/ca.crt",
		"VECTIS_SECRETS_PROVIDERS_KNOX_CLIENT_CERT_FILE": opts.KnoxCertMountPath + "/client.crt",
		"VECTIS_SECRETS_PROVIDERS_KNOX_CLIENT_KEY_FILE":  opts.KnoxCertMountPath + "/client.key",
		"VECTIS_SECRETS_POLICY_ALLOW":                    strings.Join([]string{smokeKnoxPolicyAllowEncrypted, smokeKnoxPolicyAllowKnox}, ","),
	}
}

func patchSmokeSecretsKnoxCertVolume(ctx context.Context, opts SmokeOptions) error {
	patch := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []map[string]any{
						{
							"name": smokeSecretsContainerName,
							"volumeMounts": []map[string]any{
								{
									"name":      smokeKnoxCertVolumeName,
									"mountPath": opts.KnoxCertMountPath,
									"readOnly":  true,
								},
							},
						},
					},
					"volumes": []map[string]any{
						{
							"name": smokeKnoxCertVolumeName,
							"secret": map[string]string{
								"secretName": smokeKnoxCertSecretName,
							},
						},
					},
				},
			},
		},
	}

	return patchSmokeWorkload(ctx, opts, smokeSecretsStatefulSetName, "Knox cert volume", patch)
}

func removeSmokeSecretsKnoxCertVolume(opts SmokeOptions) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.Wait)
	defer cancel()

	patch := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []map[string]any{
						{
							"name": smokeSecretsContainerName,
							"volumeMounts": []map[string]any{
								{
									"mountPath": opts.KnoxCertMountPath,
									"$patch":    "delete",
								},
							},
						},
					},
					"volumes": []map[string]any{
						{
							"name":   smokeKnoxCertVolumeName,
							"$patch": "delete",
						},
					},
				},
			},
		},
	}

	if err := patchSmokeWorkload(ctx, opts, smokeSecretsStatefulSetName, "remove Knox cert volume", patch); err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: remove Knox cert volume failed: %v\n", err)
	}
}

func patchSmokeWorkload(ctx context.Context, opts SmokeOptions, workload, description string, patch map[string]any) error {
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal %s patch: %w", description, err)
	}

	fmt.Fprintf(opts.Stdout, "Patching %s for %s\n", workload, description)
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "patch", workload, "--type=strategic", "--patch", string(data))
	if err != nil {
		return fmt.Errorf("patch %s for %s: %w: %s%s", workload, description, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeWorkload(ctx, opts, workload); err != nil {
		return fmt.Errorf("wait for %s after %s patch: %w", workload, description, err)
	}

	return nil
}

func writeSmokeKnoxCertFiles(certs smokeKnoxCertBundle) (string, func(), error) {
	dir, err := os.MkdirTemp("", "vectis-knox-smoke-*")
	if err != nil {
		return "", nil, err
	}

	cleanup := func() {
		_ = os.RemoveAll(dir)
	}

	for _, file := range []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"ca.crt", certs.CA, 0o644},
		{"server.crt", certs.ServerCert, 0o644},
		{"server.key", certs.ServerKey, 0o600},
		{"client.crt", certs.ClientCert, 0o644},
		{"client.key", certs.ClientKey, 0o600},
	} {
		if err := os.WriteFile(filepath.Join(dir, file.name), file.data, file.mode); err != nil {
			cleanup()
			return "", nil, err
		}
	}

	return dir, cleanup, nil
}

func newSmokeKnoxCertBundle(opts SmokeOptions) (smokeKnoxCertBundle, error) {
	principalID, err := smokeKnoxPrincipalID(opts.KnoxAuthToken)
	if err != nil {
		return smokeKnoxCertBundle{}, err
	}

	caPEM, caKey, err := newSmokeKnoxCertificateAuthority(smokeKnoxCertificateTTL)
	if err != nil {
		return smokeKnoxCertBundle{}, err
	}

	serverCertPEM, serverKeyPEM, err := newSmokeKnoxLeafCertificate(caPEM, caKey, smokeKnoxLeafCertificateOptions{
		CommonName: smokeKnoxServiceDNSName,
		DNSNames: []string{
			"localhost",
			smokeKnoxServiceDNSName,
			smokeKnoxServiceDNSName + "." + opts.Namespace,
			smokeKnoxServiceDNSName + "." + opts.Namespace + ".svc",
		},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		TTL:       smokeKnoxCertificateTTL,
	})

	if err != nil {
		return smokeKnoxCertBundle{}, err
	}

	clientCertPEM, clientKeyPEM, err := newSmokeKnoxLeafCertificate(caPEM, caKey, smokeKnoxLeafCertificateOptions{
		CommonName: principalID,
		DNSNames:   []string{principalID},
		KeyUsages:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		TTL:        smokeKnoxCertificateTTL,
	})

	if err != nil {
		return smokeKnoxCertBundle{}, err
	}

	return smokeKnoxCertBundle{
		CA:         caPEM,
		ServerCert: serverCertPEM,
		ServerKey:  serverKeyPEM,
		ClientCert: clientCertPEM,
		ClientKey:  clientKeyPEM,
	}, nil
}

func smokeKnoxPrincipalID(authToken string) (string, error) {
	authToken = strings.TrimSpace(authToken)
	if len(authToken) < 3 || authToken[0] != '0' || authToken[1] != 't' {
		return "", fmt.Errorf("Knox auth token must use Knox machine auth form 0t<principal>")
	}

	return authToken[2:], nil
}

func newSmokeKnoxCertificateAuthority(ttl time.Duration) ([]byte, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	template := smokeKnoxCertificateTemplate("vectis-knox-smoke-ca", ttl)
	template.IsCA = true
	template.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	template.BasicConstraintsValid = true

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key, nil
}

type smokeKnoxLeafCertificateOptions struct {
	CommonName  string
	DNSNames    []string
	IPAddresses []net.IP
	KeyUsages   []x509.ExtKeyUsage
	TTL         time.Duration
}

func newSmokeKnoxLeafCertificate(caPEM []byte, caKey *ecdsa.PrivateKey, opts smokeKnoxLeafCertificateOptions) ([]byte, []byte, error) {
	block, _ := pem.Decode(caPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("generated CA PEM could not be decoded")
	}

	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	template := smokeKnoxCertificateTemplate(opts.CommonName, opts.TTL)
	template.KeyUsage = x509.KeyUsageDigitalSignature
	template.ExtKeyUsage = opts.KeyUsages
	template.DNSNames = opts.DNSNames
	template.IPAddresses = opts.IPAddresses

	der, err := x509.CreateCertificate(rand.Reader, &template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}),
		nil
}

func smokeKnoxCertificateTemplate(commonName string, ttl time.Duration) x509.Certificate {
	notBefore := time.Now().Add(-time.Minute)
	notAfter := time.Now().Add(ttl)
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		panic(err)
	}

	return x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,
	}
}
