package upload

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nguyengg/go-aws-commons/s3writer"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) upload(ctx context.Context, name string) error {
	// f will be either name opened as-is, or a new archive created from compressing directory with that name.
	// if the latter is the case, the file will be deleted upon return.
	var (
		f           *os.File
		size        int64
		contentType *string
		checksum    string
	)

	// name can either be a file or a directory, so use stat to determine what to do.
	// if it's a directory, compress it and the resulting archive will be deleted upon return.
	switch fi, err := os.Stat(name); {
	case err != nil:
		return fmt.Errorf(`stat file "%s" error: %w`, name, err)

	case fi.IsDir():
		f, size, contentType, checksum, err = c.compress(ctx, name)
		if err != nil {
			return fmt.Errorf(`compress directory "%s" error: %w`, name, err)
		}

		defer func() {
			_, _ = f.Close(), os.Remove(f.Name())
		}()

	default:
		f, size, contentType, checksum, err = c.inspect(ctx, name)
		if err != nil {
			return fmt.Errorf(`inspect file "%s" error: %w`, name, err)
		}

		defer f.Close()
	}

	// use the name of the archive (in the case of directory) to have meaningful extensions.
	stem, ext := util.StemAndExt(f.Name())
	key := c.Prefix + stem + ext
	m := manifest.Manifest{
		Bucket:              c.Bucket,
		Key:                 key,
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		Size:                size,
	}

	c.logger.Printf(`uploading to "s3://%s/%s"`, c.Bucket, key)

	w, err := s3writer.New(ctx, c.client, &s3.PutObjectInput{
		Bucket:              aws.String(c.Bucket),
		Key:                 aws.String(key),
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		ContentType:         contentType,
		StorageClass:        s3types.StorageClassIntelligentTiering,
		Metadata:            map[string]string{"checksum": checksum},
	}, func(options *s3writer.Options) {
		options.Concurrency = c.MaxConcurrency
	}, s3writer.WithProgressBar(size))
	if err != nil {
		return fmt.Errorf("create s3 writer error: %w", err)
	}
	if _, err = w.ReadFrom(f); err != nil {
		return fmt.Errorf("upload to s3 error: %w", err)
	}
	if err = w.Close(); err != nil {
		return fmt.Errorf("close s3 writer error: %w", err)
	}

	c.logger.Printf("done uploading")

	// now generate the local .s3 file that contains the S3 URI. if writing to file fails, prints the JSON content
	// to standard output so that they can be saved manually later.
	mf, err := util.OpenExclFile(".", stem, ext+".s3", 0666)
	if err != nil {
		_ = m.MarshalTo(os.Stdout)
		return fmt.Errorf("create manifest file error: %w", err)
	}
	if err, _ = m.MarshalTo(mf), mf.Close(); err != nil {
		_ = m.MarshalTo(os.Stdout)
		return fmt.Errorf("write manifest error: %w", err)
	}

	c.logger.Printf(`wrote to manifest "%s"`, mf.Name())

	return nil
}
