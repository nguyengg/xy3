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
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) download(ctx context.Context, man manifest.Manifest) (name string, err error) {
	stem, ext := util.StemAndExt(man.Key)
	f, err := os.CreateTemp(".", stem+"-*"+ext)
	if err != nil {
		return "", fmt.Errorf("create temp file error: %w", err)
	}

	name = f.Name()
	defer func() {
		if err := f.Close(); err != nil {
			c.logger.Printf(`close temp file "%s" error: %v`, name, err)
		}
	}()

	c.logger.Printf(`downloading to "%s"`, name)

	r, err := s3reader.New(ctx, c.client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	}, func(options *s3reader.Options) {
		options.Concurrency = c.MaxConcurrency
	}, s3reader.WithProgressBar())
	if err != nil {
		return "", fmt.Errorf("create s3 reader error: %w", err)
	}

	if verifier, _ := sri.NewVerifier(man.Checksum); verifier != nil {
		if _, err = r.WriteTo(io.MultiWriter(f, verifier)); err != nil {
			return name, err
		} else if verifier.SumAndVerify(nil) {
			c.logger.Printf("done downloading; checksum matches")
		} else {
			c.logger.Printf("done downloading; checksum does not match: expect %s, got %s", man.Checksum, verifier.SumToString(nil))
		}
	} else if _, err = r.WriteTo(f); err != nil {
		return name, err
	} else {
		c.logger.Printf("done downloading; no checksum to verify")
	}

	return
}
