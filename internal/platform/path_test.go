package platform

import "testing"

func TestIsCrossPlatformAbsPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "relative", path: "scripts/build", want: false},
		{name: "posix absolute", path: "/tmp", want: true},
		{name: "windows rooted", path: `\tmp`, want: true},
		{name: "windows drive absolute", path: `C:\tmp`, want: true},
		{name: "windows drive slash", path: "C:/tmp", want: true},
		{name: "windows drive relative", path: "C:tmp", want: true},
		{name: "unc", path: `\\server\share`, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCrossPlatformAbsPath(tt.path); got != tt.want {
				t.Fatalf("IsCrossPlatformAbsPath(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}
