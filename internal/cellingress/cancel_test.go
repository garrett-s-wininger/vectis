package cellingress

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestCancelRequiresProducerIdentityAndOwningCell(t *testing.T) {
	for _, tc := range []struct {
		name, producer, owner string
		want                  int
		stale                 bool
	}{
		{"missing identity", "", "iad-a", http.StatusForbidden, false},
		{"wrong identity", "spiffe://vectis.internal/service/worker", "iad-a", http.StatusForbidden, false},
		{"wrong cell", "spiffe://vectis.internal/service/api", "pdx-b", http.StatusNotFound, false},
		{"stale generation", "spiffe://vectis.internal/service/api", "iad-a", http.StatusConflict, true},
		{"authorized", "spiffe://vectis.internal/service/api", "iad-a", http.StatusAccepted, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)

			viper.Set("service_identity.cell_ingress_allowed_producer_identities", []string{"spiffe://vectis.internal/service/api"})
			runs := dal.NewSQLRepositoriesWithCellID(dbtest.NewTestDB(t), tc.owner).Runs()
			runID, _, err := runs.CreateRun(context.Background(), "job", nil, 1)
			if err != nil {
				t.Fatal(err)
			}

			root, err := runs.GetPendingExecution(context.Background(), runID)
			if err != nil {
				t.Fatal(err)
			}

			rootID := root.ExecutionID
			if tc.stale {
				rootID = "superseded-root"
			}

			srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
			srv.SetRunStore(runs)
			req := newCellIngressRequest(http.MethodPost, "/cell/v1/runs/cancel", strings.NewReader(fmt.Sprintf(`{"run_id":%q,"root_execution_id":%q}`, runID, rootID)))
			req.Header.Set("Content-Type", "application/json")
			if tc.producer != "" {
				req.TLS = tlsStateWithURI(t, tc.producer)
			}

			response := httptest.NewRecorder()
			srv.ServeHTTP(response, req)
			if response.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.want, response.Body.String())
			}

			requested, err := runs.RunCancelRequested(context.Background(), runID)
			if err != nil || requested != (tc.want == http.StatusAccepted) {
				t.Fatalf("intent=%v error=%v", requested, err)
			}
		})
	}
}
