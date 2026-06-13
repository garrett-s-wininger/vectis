package action

import (
	"reflect"
	"testing"
)

func TestNormalizeSupportedIsolationLevels(t *testing.T) {
	tests := []struct {
		name    string
		levels  []string
		want    []string
		wantErr bool
	}{
		{
			name: "empty",
		},
		{
			name:   "normalizes and deduplicates",
			levels: []string{" HOST ", "vm", "host"},
			want:   []string{IsolationHost, IsolationVM},
		},
		{
			name:    "blank entry",
			levels:  []string{IsolationHost, " "},
			wantErr: true,
		},
		{
			name:    "unsupported entry",
			levels:  []string{IsolationHost, "container"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeSupportedIsolationLevels(tt.levels)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NormalizeSupportedIsolationLevels() error = %v, wantErr=%v", err, tt.wantErr)
			}

			if err != nil {
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("NormalizeSupportedIsolationLevels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsSupportedIsolationLevelRequiresExplicitLevel(t *testing.T) {
	if !IsSupportedIsolation("") {
		t.Fatal("empty job isolation should remain supported")
	}

	if IsSupportedIsolationLevel("") {
		t.Fatal("empty explicit isolation level should not be supported")
	}

	if !IsSupportedIsolationLevel(IsolationHost) || !IsSupportedIsolationLevel(IsolationVM) {
		t.Fatal("host and vm should be supported explicit isolation levels")
	}
}
