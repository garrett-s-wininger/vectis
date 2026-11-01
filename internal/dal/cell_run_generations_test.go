package dal

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "vectis/api/gen/go"
	"vectis/internal/dispatchmeta"
)

func TestAcceptedRunPayloadOnlyAllowsDispatchMetadataChanges(t *testing.T) {
	makeRequest := func(mutate func(*pb.JobRequest, map[string]any)) string {
		req := &pb.JobRequest{Job: &pb.Job{Id: proto.String("job"), RunId: proto.String("run"), Root: &pb.Node{Uses: proto.String("builtins/script"), With: map[string]string{"script": "echo frozen"}}}, Metadata: map[string]string{"user-key": "frozen"}}
		env := map[string]any{"execution_id": "old", "task_attempt": 1, "definition_hash": "frozen-hash", "metadata": map[string]any{"secret-ref": "unchanged"}, "action_locks": []any{map[string]any{"digest": "sha256:frozen"}}}
		if mutate != nil {
			mutate(req, env)
		}

		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}

		req.Metadata[dispatchmeta.ExecutionEnvelopeMetadataKey] = string(raw)
		raw, err = protojson.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}

		return string(raw)
	}

	frozen := makeRequest(nil)
	tests := []struct {
		name    string
		mutate  func(*pb.JobRequest, map[string]any)
		allowed bool
	}{
		{"delivery", func(req *pb.JobRequest, env map[string]any) {
			req.Job.DeliveryId = proto.String("new-delivery")
			req.Metadata["traceparent"] = "new-trace"
			env["execution_id"] = "new"
			env["task_attempt"] = 2
			env["created_at_unix_nano"] = 456
		}, true},
		{"command", func(req *pb.JobRequest, _ map[string]any) { req.Job.Root.With["script"] = "different command" }, false},
		{"user metadata", func(req *pb.JobRequest, _ map[string]any) { req.Metadata["user-key"] = "different" }, false},
		{"action lock", func(_ *pb.JobRequest, env map[string]any) {
			env["action_locks"] = []any{map[string]any{"digest": "sha256:different"}}
		}, false},
		{"definition", func(_ *pb.JobRequest, env map[string]any) { env["definition_hash"] = "different" }, false},
		{"envelope metadata", func(_ *pb.JobRequest, env map[string]any) {
			env["metadata"] = map[string]any{"secret-ref": "different"}
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameAcceptedRunPayload(frozen, makeRequest(tt.mutate)); got != tt.allowed {
				t.Fatalf("payload allowed=%v, want %v", got, tt.allowed)
			}
		})
	}

	if sameAcceptedRunPayload(frozen, "not json") {
		t.Fatal("invalid payload accepted")
	}
}
