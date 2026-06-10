package job_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	api "vectis/api/gen/go"
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
    "uses": "builtins/shell",
    "with": {"command": "test -f \"$VECTIS_SECRETS_DIR/token\""}
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
  "root": {"id": "root", "uses": "builtins/shell", "with": {"command": "true"}}
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

	for _, path := range examples {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			body := readExampleFile(t, path)

			var decoded api.Job
			if err := job.DecodeDefinitionJSON(body, &decoded); err != nil {
				t.Fatalf("DecodeDefinitionJSON: %v", err)
			}

			if err := validation.ValidateJob(&decoded, validation.Options{RequireJobID: true}); err != nil {
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
