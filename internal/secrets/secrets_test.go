package secrets

import (
	"testing"

	api "vectis/api/gen/go"
)

func TestReferencesForTaskFiltersTaskScopedSecrets(t *testing.T) {
	t.Parallel()

	fileType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
	job := &api.Job{
		Secrets: []*api.SecretReference{
			{
				Id:  strp("all"),
				Ref: strp("encryptedfs://team/all"),
				Delivery: &api.SecretDelivery{
					Type: &fileType,
					Path: strp("all"),
				},
			},
			{
				Id:       strp("build"),
				Ref:      strp("encryptedfs://team/build"),
				TaskKeys: []string{"build"},
				Delivery: &api.SecretDelivery{
					Type: &fileType,
					Path: strp("build"),
				},
			},
			{
				Id:       strp("test"),
				Ref:      strp("encryptedfs://team/test"),
				TaskKeys: []string{"test"},
				Delivery: &api.SecretDelivery{
					Type: &fileType,
					Path: strp("test"),
				},
			},
		},
	}

	refs := ReferencesForTask(job, "build")
	if len(refs) != 2 {
		t.Fatalf("refs = %+v, want global and build", refs)
	}

	if refs[0].ID != "all" || refs[1].ID != "build" {
		t.Fatalf("refs = %+v", refs)
	}
}

func TestReferencesForTaskMatchesRootNodeIDForRootExecution(t *testing.T) {
	t.Parallel()

	fileType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
	job := &api.Job{
		Root: &api.Node{
			Id: strp("verify-secret"),
		},
		Secrets: []*api.SecretReference{
			{
				Id:       strp("smoke-token"),
				Ref:      strp("encryptedfs://team/smoke-token"),
				TaskKeys: []string{"verify-secret"},
				Delivery: &api.SecretDelivery{
					Type: &fileType,
					Path: strp("smoke/token"),
				},
			},
		},
	}

	refs := ReferencesForTask(job, "root")
	if len(refs) != 1 || refs[0].ID != "smoke-token" {
		t.Fatalf("refs = %+v, want root-node scoped secret", refs)
	}
}
