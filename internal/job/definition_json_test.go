package job_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/job"
	"vectis/internal/job/validation"
)

func TestDecodeDefinitionJSONAcceptsFriendlySecretDeliveryType(t *testing.T) {
	t.Parallel()

	body := []byte(`{
  "id": "secret-example",
  "secrets": [{
    "id": "token",
    "ref": "encryptedfs://team/token",
    "delivery": {"type": "file", "path": "token"},
    "task_keys": ["verify"]
  }],
  "root": {
    "id": "verify",
    "uses": "builtins/script",
    "with":{"script": "test -f \"$VECTIS_SECRETS_DIR/token\""}
  }
}`)

	var decoded api.Job
	if err := job.DecodeDefinitionJSON(body, &decoded); err != nil {
		t.Fatalf("DecodeDefinitionJSON: %v", err)
	}

	if got := decoded.GetSecrets()[0].GetDelivery().GetType(); got != api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE {
		t.Fatalf("delivery type = %v, want file", got)
	}
}

func TestDecodeDefinitionJSONRejectsUnknownSecretDeliveryTypeAlias(t *testing.T) {
	t.Parallel()

	body := []byte(`{
  "id": "bad-secret",
  "secrets": [{
    "id": "token",
    "ref": "encryptedfs://team/token",
    "delivery": {"type": "env", "path": "token"}
  }],
  "root": {"id": "root", "uses": "builtins/script", "with":{"script": "true"}}
}`)

	var decoded api.Job
	err := job.DecodeDefinitionJSON(body, &decoded)
	if err == nil || !strings.Contains(err.Error(), `unknown secret delivery type "env"`) {
		t.Fatalf("DecodeDefinitionJSON error = %v, want unknown delivery type", err)
	}
}

func TestExamplesDecodeAndValidate(t *testing.T) {
	examples, err := filepath.Glob("../../examples/*.json")
	if err != nil {
		t.Fatalf("glob examples: %v", err)
	}

	if len(examples) == 0 {
		t.Fatal("no example jobs found")
	}

	resolver := exampleValidationResolver(t)
	for _, path := range examples {
		t.Run(filepath.Base(path), func(t *testing.T) {
			body := readExampleFile(t, path)

			var decoded api.Job
			if err := job.DecodeDefinitionJSON(body, &decoded); err != nil {
				t.Fatalf("DecodeDefinitionJSON: %v", err)
			}

			if err := validation.ValidateJob(&decoded, validation.Options{RequireJobID: true, Resolver: resolver}); err != nil {
				t.Fatalf("ValidateJob: %v", err)
			}
		})
	}
}

func readExampleFile(t *testing.T, path string) []byte {
	t.Helper()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	return body
}

func exampleValidationResolver(t *testing.T) action.Resolver {
	t.Helper()

	source, err := actionregistry.NewLocalManifestSource(filepath.Clean("../../examples/actions"))
	if err != nil {
		t.Fatalf("NewLocalManifestSource: %v", err)
	}

	extensionSource, err := actionregistry.NewLocalManifestSource(filepath.Clean("../../extensions/actions"))
	if err != nil {
		t.Fatalf("NewLocalManifestSource extensions: %v", err)
	}

	resolver, err := job.NewActionResolver(actionregistry.NewCompositeResolver(builtins.NewRegistry(), source, extensionSource), nil)
	if err != nil {
		t.Fatalf("NewActionResolver: %v", err)
	}

	return resolver
}
