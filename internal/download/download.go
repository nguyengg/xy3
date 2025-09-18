package download

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

func (c *Command) download(ctx context.Context, name string) error {
	man, err := manifest.UnmarshalFromFile(name)
	if err != nil {
		return fmt.Errorf("read mannifest error: %w", err)
	}

	stem, ext := util.StemAndExt(man.Key)
	verifier, _ := sri.NewVerifier(man.Checksum)

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

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file complete, clean up by deleting the local file.
	file, err := util.OpenExclFile(".", stem, ext, 0666)
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
	}, s3reader.WithProgressBar())
	if err != nil {
		return fmt.Errorf("create s3 reader error: %w", err)
	}
	defer r.Close()

	if verifier != nil {
		_, err = r.WriteTo(io.MultiWriter(file, verifier))
	} else {
		_, err = r.WriteTo(file)
	}

	if err != nil {
		return err
	}

	success = true

	if verifier == nil {
		c.logger.Printf("done downloading; no checksum to verify")
		return nil
	}

	if verifier.SumAndVerify(nil) {
		c.logger.Printf("done downloading; checksum matches")
	} else {
		c.logger.Printf("done downloading; checksum does not match: expect %s, got %s", man.Checksum, verifier.SumToString(nil))
	}

	return nil
}
