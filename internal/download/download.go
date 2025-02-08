package download

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/namedhash"
	"github.com/schollz/progressbar/v3"
)

func (c *Command) download(ctx context.Context, name string) error {
	file, err := os.Open(name)
	if err != nil {
		return fmt.Errorf("open file error: %w", err)
	}
	man, err := manifest.UnmarshalFrom(file)
	if _ = file.Close(); err != nil {
		return err
	}
	basename := filepath.Base(man.Key)
	ext := filepath.Ext(basename)

	// while downloading, also computes checksum to verify against the downloaded content.
	h, err := namedhash.NewFromChecksumString(man.Checksum)
	if err != nil {
		return err
	}

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file complete, clean up by deleting the local file.
	file, err = xy3.OpenExclFile(strings.TrimSuffix(basename, ext), ext)
	success := false
	defer func(file *os.File) {
		if name, _ = file.Name(), file.Close(); !success {
			c.logger.Printf(`deleting file "%s"`, name)
			if err = os.Remove(name); err != nil {
				c.logger.Printf("delete file error: %v", err)
			}
		}
	}(file)

	c.logger.Printf(`downloading from "s3://%s/%s" to "%s"`, man.Bucket, man.Key, file.Name())

	var w io.Writer = file
	if h != nil {
		w = io.MultiWriter(file, h)
	}

	if err = xy3.Download(ctx, c.client, man.Bucket, man.Key, w, func(options *xy3.DownloadOptions) {
		options.Concurrency = c.MaxConcurrency
		options.ModifyHeadObjectInput = func(input *s3.HeadObjectInput) {
			input.ExpectedBucketOwner = man.ExpectedBucketOwner
		}
		options.ModifyGetObjectInput = func(input *s3.GetObjectInput) {
			input.ExpectedBucketOwner = man.ExpectedBucketOwner
		}

		var bar *progressbar.ProgressBar
		var completedPartCount int
		options.PostGetPart = func(data []byte, size int64, partNumber, partCount int) {
			if bar == nil {
				bar = internal.DefaultBytes(size, "downloading")
			}
			if completedPartCount++; completedPartCount == partCount {
				_ = bar.Close()
			} else {
				_ = bar.Add64(int64(len(data)))
			}
		}
	}); err != nil {
		return err
	}
	success = true

	if h == nil {
		c.logger.Printf("done downloading; no checksum to verify")
		return nil
	}

	if actual := h.SumToString(nil); man.Checksum != actual {
		c.logger.Printf("done downloading; checksum does not match: expect %s, got %s", man.Checksum, actual)
	} else {
		c.logger.Printf("done downloading; checksum matches")
	}

	return nil
}
