package upload

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/zipper"
	"os"
	"path/filepath"
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// All files in the archive include root's basename in its path, meaning the top-level file of the archive output is
// the root directory itself.
func (c *Command) compress(ctx context.Context, root string) (name string, contentType *string, err error) {
	base := filepath.Base(root)
	contentType = aws.String("application/zip")

	// a new file will always be created, and if the operation fails, the file will be auto deleted.
	out, err := internal.OpenExclFile(base, ".zip")
	if err != nil {
		err = fmt.Errorf("create archive error: %w", err)
		return
	}

	name = out.Name()
	defer func() {
		if _ = out.Close(); err != nil {
			_ = os.Remove(name)
			name = ""
		}
	}()

	z := zipper.New()
	z.ProgressReporter, err = zipper.NewProgressBarReporter(ctx, root, nil)
	if err == nil {
		err = z.CompressDir(ctx, root, out, false)
	}
	return
}
