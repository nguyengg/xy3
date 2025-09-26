package upload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/util"
)

// compressDir creates a new archive and compresses all files recursively starting at root.
//
// On success, return the name of the archive.
func (c *Command) compressDir(ctx context.Context, dir string) (name string, contentType *string, err error) {
	alg := xy3.DefaultAlgorithmName
	comp := xy3.NewCompressorFromName(alg)
	ext := comp.ArchiveExt()

	f, err := util.OpenExclFile(".", filepath.Base(dir), ext, 0666)
	if err != nil {
		return "", nil, fmt.Errorf("create archive error: %w", err)
	}
	defer f.Close()

	if err = xy3.CompressDir(ctx, dir, f, func(opts *xy3.CompressOptions) {
		opts.Algorithm = alg
		opts.MaxConcurrency = c.MaxConcurrency
	}); err != nil {
		_, _ = f.Close(), os.Remove(f.Name())
		return "", nil, err
	}

	if err = f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", nil, fmt.Errorf("close archive error: %w", err)
	}

	return f.Name(), aws.String(comp.ContentType()), nil
}
