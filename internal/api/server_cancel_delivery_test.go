package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/spf13/viper"
	"vectis/internal/dal"
	"vectis/internal/database"
)

func TestCancelAcknowledgesOnlyAfterOwningCellAccepts(t *testing.T) {
	for _, status := range []int{http.StatusAccepted, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server, _, _, db := setupTestServer(t)
			t.Setenv(database.EnvGlobalDatabaseDSN, filepath.Join(t.TempDir(), "global.db"))
			t.Setenv(database.EnvCellDatabaseDSN, filepath.Join(t.TempDir(), "cell.db"))
			insertDefinitionSnapshotForTest(t, db, "cancel-cell", `{"id":"cancel-cell"}`)

			runs := dal.NewSQLRepositories(db).Runs()
			runID, _, err := runs.CreateRun(context.Background(), "cancel-cell", nil, 1)
			if err != nil {
				t.Fatal(err)
			}

			var called atomic.Bool
			cell := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called.Store(true)
				if r.Method != http.MethodPost || r.URL.Path != "/cell/v1/runs/cancel" {
					t.Errorf("request=%s %s", r.Method, r.URL.Path)
				}

				var body struct {
					RunID string `json:"run_id"`
				}

				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RunID != runID {
					t.Errorf("body=%+v error=%v", body, err)
				}

				w.WriteHeader(status)
			}))
			defer cell.Close()

			viper.Set("cell_ingress_endpoints", []string{"local=" + cell.URL})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/cancel", nil)
			req.SetPathValue("id", runID)
			recorder := httptest.NewRecorder()
			server.CancelRun(recorder, req)

			if !called.Load() || recorder.Code != status {
				t.Fatalf("called=%v status=%d body=%s", called.Load(), recorder.Code, recorder.Body.String())
			}

			requested, err := runs.RunCancelRequested(context.Background(), runID)
			if err != nil || requested != (status == http.StatusAccepted) {
				t.Fatalf("global intent=%v error=%v", requested, err)
			}
		})
	}
}
