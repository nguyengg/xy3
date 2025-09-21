package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/go-aws-commons/sri"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/extract"
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

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file complete, clean up by deleting the local file.
	file, err := util.OpenExclFile(".", stem, ext, 0666)
	if err != nil {
		return fmt.Errorf("create output file error: %w", err)
	}

	c.logger.Printf(`downloading to "%s"`, file.Name())

	success := false
	defer func() {
		if name, _ = file.Name(), file.Close(); !success {
			c.logger.Printf(`deleting file "%s"`, name)
			if err = os.Remove(name); err != nil {
				c.logger.Printf("delete file error: %v", err)
			}
		}
	}()

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
	} else if verifier.SumAndVerify(nil) {
		c.logger.Printf("done downloading; checksum matches")
	} else {
		c.logger.Printf("done downloading; checksum does not match: expect %s, got %s", man.Checksum, verifier.SumToString(nil))
	}

	if c.Extract {
		return c.extract(ctx, file, ext)
	}

	return nil
}

func (c *Command) extract(ctx context.Context, file *os.File, ext string) (err error) {
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		c.logger.Printf("seek start file error, will not extract: %v", err)
		return nil
	}

	bar := internal.DefaultBytes(-1, "extracting")
	if err, _ = extract.Extract(ctx, file, ext, func(opts *extract.Options) {
		opts.ProgressBar = bar
	}), bar.Close(); err != nil {
		if errors.Is(err, extract.ErrUnknownArchiveExtension) {
			c.logger.Printf("file is not eligible for auto-extracting: %v", err)
			return nil
		}

		return err
	}

	// if extraction is successful, delete the archive.
	c.logger.Printf("extract success, deleting archive")
	if _, err = file.Close(), os.Remove(file.Name()); err != nil {
		c.logger.Printf("delete file error: %v", err)
	}

	return nil
}
