package download

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/cksum"
	"github.com/nguyengg/xy3/internal/manifest"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	h, err := cksum.NewFromChecksumString(man.Checksum)
	if err != nil {
		return err
	}

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file complete, clean up by deleting the local file.
	file, err = internal.OpenExclFile(strings.TrimSuffix(basename, ext), ext)
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

	if err = xy3.Download(ctx, c.client, man.Bucket, man.Key, w, func(downloader *xy3.Downloader) {
		downloader.Concurrency = c.MaxConcurrency
		downloader.ModifyHeadObjectInput = func(input *s3.HeadObjectInput) {
			input.ExpectedBucketOwner = man.ExpectedBucketOwner
		}
		downloader.ModifyGetObjectInput = func(input *s3.GetObjectInput) {
			input.ExpectedBucketOwner = man.ExpectedBucketOwner
		}

		bar := internal.DefaultBytes(man.Size, "downloading")

		var completedPartCount int
		downloader.PostGetPart = func(data []byte, partNumber, partCount int) {
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

	if actual := h.SumToChecksumString(nil); man.Checksum != actual {
		c.logger.Printf("done downloading; checksum does not match: expect %s, got %s", man.Checksum, actual)
	} else {
		c.logger.Printf("done downloading; checksum matches")
	}

	return nil
}
