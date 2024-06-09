package upload

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/dustin/go-humanize"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/zipper"
	"golang.org/x/time/rate"
	"log"
	"os"
	"path/filepath"
	"time"
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// All files in the archive include root's basename in its path, meaning the top-level file of the archive output is
// the root directory itself.
func (c *Command) compress(ctx context.Context, logger *log.Logger, root string) (name string, contentType *string, err error) {
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

	// report compress progress every few seconds. for each file, if it takes longer than some seconds to compress then
	// start reporting progress for that individual file every few seconds as well.
	var (
		sometimes    = rate.Sometimes{Interval: 5 * time.Second}
		dstSometimes = rate.Sometimes{Interval: 5 * time.Second}
		lastDst      = ""
	)

	z := zipper.New()
	z.ProgressReporter, err = zipper.NewDirectoryProgressReporter(ctx, root, func(src, dst string, written, size int64, done bool, wc, fc int) {
		if dst != lastDst {
			lastDst = dst
			dstSometimes.Do(func() {})
		}

		switch {
		case done && wc == fc:
			logger.Printf("[%d/%d] done compressing all files", wc, fc)
		case done:
			sometimes.Do(func() {
				logger.Printf(`[%d/%d] done compressing "%s"`, wc, fc, dst)
			})
		default:
			dstSometimes.Do(func() {
				logger.Printf(`[%d/%d] compressed %.2f%% of "%s" (%s) so far`, wc, fc, 100.0*float64(written)/float64(size), dst, humanize.Bytes(uint64(size)))
			})
		}
	})
	if err == nil {
		err = z.CompressDir(ctx, root, out, false)
	}
	return
}
