package upload

import (
	"compress/flate"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3/util"
	"github.com/nguyengg/xy3/zipper"
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// All files in the archive include root's basename in its path, meaning the top-level file of the archive output is
// the root directory itself. The returned file is open for reading at read offset 0 (start of file) unless there was an
// error.
func (c *Command) compress(ctx context.Context, root string) (f *os.File, contentType *string, err error) {
	f, err = util.OpenExclFile(".", filepath.Base(root), ".zip", 0666)
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

	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		_, _ = f.Close(), os.Remove(f.Name())
		return nil, nil, err
	}

	return f, aws.String("application/zip"), nil
}
