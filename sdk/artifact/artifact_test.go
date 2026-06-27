package artifact

import (
	"errors"
	"testing"
)

func TestValidateBlobDescriptor(t *testing.T) {
	digest := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	valid := DescriptorForSHA256(digest, 5)
	if err := ValidateBlobDescriptor(valid); err != nil {
		t.Fatalf("ValidateBlobDescriptor valid: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*BlobDescriptor)
		want   error
	}{
		{name: "bad algorithm", mutate: func(d *BlobDescriptor) { d.Algorithm = "md5" }, want: ErrInvalidBlobDescriptor},
		{name: "bad key", mutate: func(d *BlobDescriptor) { d.Key = "md5:" + digest }, want: ErrInvalidBlobKey},
		{name: "bad digest", mutate: func(d *BlobDescriptor) { d.Digest = "bad" }, want: ErrInvalidDigest},
		{name: "key digest mismatch", mutate: func(d *BlobDescriptor) { d.Digest = "486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7" }, want: ErrInvalidBlobDescriptor},
		{name: "negative size", mutate: func(d *BlobDescriptor) { d.Size = -1 }, want: ErrInvalidBlobDescriptor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desc := valid
			tt.mutate(&desc)
			if err := ValidateBlobDescriptor(desc); !errors.Is(err, tt.want) {
				t.Fatalf("ValidateBlobDescriptor error = %v, want %v", err, tt.want)
			}
		})
	}
}
