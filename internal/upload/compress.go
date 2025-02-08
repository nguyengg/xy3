package upload

import (
	"compress/flate"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/zipper"
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// All files in the archive include root's basename in its path, meaning the top-level file of the archive output is
// the root directory itself.
func (c *Command) compress(ctx context.Context, root string) (name string, contentType *string, err error) {
	f, err := xy3.OpenExclFile(filepath.Base(root), ".zip")
	if err != nil {
		err = fmt.Errorf("create archive error: %w", err)
		return
	}

	var pr zipper.ProgressReporter
	if pr, err = zipper.NewProgressBarReporter(ctx, root, nil); err == nil {
		err = zipper.CompressDir(ctx, root, f, func(options *zipper.CompressDirOptions) {
			options.NewWriter = zipper.NewWriterWithDeflateLevel(flate.BestCompression)
			options.ProgressReporter = pr
		})
	}

	if _ = f.Close(); err != nil {
		_ = os.Remove(name)
		return "", nil, err
	}

	return f.Name(), aws.String("application/zip"), nil
}
