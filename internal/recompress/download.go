package recompress

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/go-aws-commons/sri"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) download(ctx context.Context, man manifest.Manifest) (*os.File, error) {
	// temp file is automatically closed and deleted on failure.
	stem, ext := util.StemAndExt(man.Key)
	file, err := os.CreateTemp(".", stem+"-*"+ext)
	if err != nil {
		return nil, fmt.Errorf("create temp file error: %w", err)
	}

	success := true
	defer func() {
		if !success || err != nil {
			_ = file.Close()
			_ = os.Remove(file.Name())
		}
	}()

	name := file.Name()
	c.logger.Printf(`downloading to "%s"`, name)

	r, err := s3reader.New(ctx, c.client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	}, func(options *s3reader.Options) {
		options.Concurrency = c.MaxConcurrency
	}, s3reader.WithProgressBar())
	if err != nil {
		return nil, fmt.Errorf("create s3 reader error: %w", err)
	}
	if _, err = r.WriteTo(file); err != nil {
		return nil, fmt.Errorf("read from s3 error: %w", err)
	}

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek start error: %w", err)
	}

	// there's a bug with using io.MultiWriter to verify checksum at the moment so do this in a separate phase.
	verifier, _ := sri.NewVerifier(man.Checksum)
	if verifier == nil {
		c.logger.Printf("done downloading; no checksum to verify")
		success = true
		return file, nil
	}

	var fi os.FileInfo
	if fi, err = file.Stat(); err != nil {
		return nil, fmt.Errorf("stat file error: %w", err)
	}

	bar := internal.DefaultBytes(fi.Size(), "verifying checksum")
	_, err = util.CopyBufferWithContext(ctx, io.MultiWriter(verifier, bar), file, nil)
	_ = bar.Close()
	if err != nil {
		return nil, fmt.Errorf("verify checksum error: %w", err)
	}

	if verifier.SumAndVerify(nil) {
		c.logger.Printf("checksum matches")
	} else {
		c.logger.Printf("checksum does not match: expect %s, got %s", man.Checksum, verifier.SumToString(nil))
	}

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek start error: %w", err)
	}

	success = true
	return file, nil
}
