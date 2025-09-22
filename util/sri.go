package util

import (
	"context"
	"fmt"
	"io"

	"github.com/nguyengg/go-aws-commons/sri"
)

// ComputeChecksum computes the default checksum (SHA-256) and rewinds the io.ReadSeeker for subsequent use.
func ComputeChecksum(ctx context.Context, src io.ReadSeeker) (string, error) {
	lev := sri.NewSha256()

	if _, err := CopyBufferWithContext(ctx, lev, src, nil); err != nil {
		return "", fmt.Errorf("compute checksum error: %w", err)
	}

	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("compute checksum error: seek start error: %w", err)
	}

	return lev.SumToString(nil), nil
}

func DefaultChecksum() sri.Hash {
	return sri.NewSha256()
}
