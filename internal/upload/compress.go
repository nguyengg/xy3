package upload

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/zipper"
	"log"
	"os"
	"path/filepath"
	"time"
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// All files in the archive include root's basename in its path, meaning the top-level file of the archive output is
// the root directory itself.
func (c *Command) compress(ctx context.Context, logger *log.Logger, root string) (name, ext string, contentType *string, err error) {
	base := filepath.Base(root)
	ext, contentType = ".zip", aws.String("application/zip")

	// a new file will always be created, and if the operation fails, the file will be auto deleted.
	out, err := internal.OpenExclFile(base, ext)
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

	// report compress progress every few seconds.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	z := zipper.New()
	z.ProgressReporter, err = zipper.NewDirectoryProgressReporter(ctx, root, func(src, dst string, written, size int64, done bool, wc, fc int) {
		if done && wc == fc {
			logger.Printf("[%d/%d] done compressing all files", wc, fc)
			return
		}

		select {
		case <-ticker.C:
			if done {
				logger.Printf("[%d/%d] done compressing %s", wc, fc, dst)
			} else {
				logger.Printf("[%d/%d] compressed %.2f%% of %s so far", wc, fc, 100.0*float64(written)/float64(size), dst)
			}
		default:
			break
		}
	})
	if err == nil {
		err = z.CompressDir(ctx, root, out, false)
	}
	return
}
