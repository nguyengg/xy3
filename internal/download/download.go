package download

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/namedhash"
	"github.com/nguyengg/xy3/s3reader"
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

	// see if the file is eligible for auto-extract.
	if c.StreamAndExtractV2 {
		if ok, err := c.streamV2(ctx, man); ok || err != nil {
			return err
		}
	} else if c.StreamAndExtract {
		if ok, err := c.stream(ctx, man); ok || err != nil {
			return err
		}
	}

	// while downloading, also computes checksum to verify against the downloaded content.
	h, err := namedhash.NewFromChecksumString(man.Checksum)
	if err != nil {
		return err
	}

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file complete, clean up by deleting the local file.
	file, err = xy3.OpenExclFile(".", strings.TrimSuffix(basename, ext), ext)
	if err != nil {
		return fmt.Errorf("create output file error: %w", err)
	}

	c.logger.Printf(`downloading to "%s"`, file.Name())

	success := false
	defer func(file *os.File) {
		if name, _ = file.Name(), file.Close(); !success {
			c.logger.Printf(`deleting file "%s"`, name)
			if err = os.Remove(name); err != nil {
				c.logger.Printf("delete file error: %v", err)
			}
		}
	}(file)

	r, err := s3reader.New(ctx, c.client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	}, func(options *s3reader.Options) {
		options.Concurrency = c.MaxConcurrency
	})
	if err != nil {
		return fmt.Errorf("create s3 reader error: %w", err)
	}
	defer r.Close()

	bar := internal.DefaultBytes(r.Size(), "downloading")
	defer bar.Close()

	if h != nil {
		_, err = r.WriteTo(io.MultiWriter(file, bar, h))
	} else {
		_, err = r.WriteTo(io.MultiWriter(file, bar))
	}

	if err != nil {
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
