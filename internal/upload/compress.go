package upload

import (
	"compress/flate"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3/util"
	"github.com/nguyengg/xy3/z"
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// All files in the archive include root's basename in its path, meaning the top-level file of the archive output is
// the root directory itself. The returned file is open for reading unless there was an error.
func (c *Command) compress(ctx context.Context, root string) (f *os.File, contentType *string, err error) {
	f, err = util.OpenExclFile(".", filepath.Base(root), ".zip", 0666)
	if err != nil {
		err = fmt.Errorf("create archive error: %w", err)
		return
	}

	var pr z.ProgressReporter
	if pr, err = z.NewProgressBarReporter(ctx, root, nil); err == nil {
		err = z.CompressDir(ctx, root, f, func(options *z.CompressDirOptions) {
			options.NewWriter = z.NewWriterWithDeflateLevel(flate.BestCompression)
			options.ProgressReporter = pr
		})
	}

	if err != nil {
		_, _ = f.Close(), os.Remove(f.Name())
		return nil, nil, err
	}

	return f, aws.String("application/zip"), nil
}
