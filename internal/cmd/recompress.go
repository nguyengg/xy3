package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jessevdk/go-flags"
	commons "github.com/nguyengg/go-aws-commons"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/config"
)

type Recompress struct {
	Profile   string `long:"profile" description:"the AWS profile to use; takes precedence over .xy3 setting"`
	Algorithm string `short:"a" long:"algorithm" choice:"zstd" choice:"zip" choice:"gzip" choice:"xz" default:"zstd"`
	MoveTo    string `long:"move-to" description:"the S3 bucket and prefix in format s3://bucket/prefix to upload the new archives to" value-name:"S3_LOCATION" required:"true"`
	Args      struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files each containing a single S3 URI" required:"yes"`
	} `positional-args:"yes"`

	bucket, prefix string
	uploadConfig   config.BucketConfig
	uploadClient   *s3.Client
	logger         *log.Logger
}

func (c *Recompress) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	if c.bucket, c.prefix, err = internal.ParseS3URI(c.MoveTo); err != nil {
		return fmt.Errorf("invalid --move-to: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	if _, err = config.LoadProfile(ctx, c.Profile); err != nil {
		return err
	}

	c.uploadConfig = config.ForBucket(c.bucket)
	c.uploadClient, err = config.NewS3ClientForBucket(ctx, c.bucket)
	if err != nil {
		return fmt.Errorf("create s3 client error: %w", err)
	}

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)
		c.logger.Printf("start recompressing")

		if err = c.recompress(ctx, string(file)); err == nil {
			c.logger.Printf("done recompressing")
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf("recompress error: %v", err)
	}

	log.Printf("successfully recompressed %d/%d files", success, n)
	return nil
}

func (c *Recompress) recompress(ctx context.Context, originalManifestName string) error {
	originalManifest, err := internal.LoadManifestFromFile(originalManifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	downloadCfg := config.ForBucket(originalManifest.Bucket)
	downloadExpectedBucketOwner := internal.FirstNonNilPtr(originalManifest.ExpectedBucketOwner, downloadCfg.ExpectedBucketOwner)

	downloadClient, err := config.NewS3ClientForBucket(ctx, originalManifest.Bucket, func(options *s3.Options) {
		// without this, getting a bunch of WARN message below:
		// WARN Response has no supported checksum. Not validating response payload.
		options.DisableLogOutputChecksumValidationSkipped = true
	})
	if err != nil {
		return fmt.Errorf("create s3 client error: %w", err)
	}

	// we'll create a temp directory to store all intermediate artifacts.
	// this temp directory is deleted only after complete success.
	stem, ext := commons.StemExt(originalManifest.Key)

	dir, err := os.MkdirTemp(".", stem+"-tmp-*")
	if err != nil {
		return fmt.Errorf("create temp dir error: %w", err)
	}

	success := false
	defer func() {
		if success {
			if err = os.RemoveAll(dir); err != nil {
				c.logger.Printf(`clean up "%s" error: %v`, dir, err)
			}
		}
	}()

	// this is essentially download and extract mode.
	// f.Close() will be called right after xy3.Download
	f, err := os.CreateTemp(dir, stem+"-*"+ext)
	if err != nil {
		return fmt.Errorf("create original file error: %w", err)
	}

	err = xy3.Download(
		ctx,
		downloadClient,
		originalManifest.Bucket,
		originalManifest.Key,
		f,
		xy3.WithExpectedBucketOwner(downloadExpectedBucketOwner),
		func(opts *xy3.DownloadOptions) {
			opts.S3ReaderOptions = func(s3readerOpts *s3reader.Options) {
				s3readerOpts.Concurrency = 0
			}

			opts.ExpectedChecksum = originalManifest.Checksum
		})
	_ = f.Close()

	if err != nil {
		if _, ok := xy3.IsErrChecksumMismatch(err); !ok {
			return err
		}

		c.logger.Print(err)
	}

	// extract to a new directory inside the working directory.
	uncompressedDir, err := xy3.Decompress(ctx, f.Name(), dir)
	if err != nil {
		return fmt.Errorf("decompress error: %w", err)
	}

	// now compress the extracted contents.
	comp := xy3.NewCompressorFromName(c.Algorithm)
	f, err = os.OpenFile(filepath.Join(dir, stem+comp.ArchiveExt()), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("create recompressed file error: %w", err)
	}

	if err, _ = xy3.CompressDir(ctx, uncompressedDir, f), f.Close(); err != nil {
		return fmt.Errorf(`compress directory "%s" error: %w`, filepath.Join(dir, uncompressedDir), err)
	}

	// reopen the file to upload.
	f, err = os.Open(f.Name())
	if err != nil {
		return fmt.Errorf("open recompressed file error: %w", err)
	}

	// bucket and key must always be present.
	bucket := c.bucket
	key := c.prefix + stem + comp.ArchiveExt()

	newMan, err := xy3.Upload(ctx, c.uploadClient, f, bucket, key, func(opts *xy3.UploadOptions) {
		opts.PutObjectInputOptions = func(input *s3.PutObjectInput) {
			input.ExpectedBucketOwner = c.uploadConfig.ExpectedBucketOwner
			input.ContentType = aws.String(comp.ContentType())
			input.StorageClass = c.uploadConfig.StorageClass
		}
	})
	_ = f.Close()
	if err != nil {
		return fmt.Errorf("upload error: %w", err)
	}

	// write manifest to a unique local .s3 file.
	f, err = commons.OpenExclFile(".", stem, comp.ArchiveExt()+".s3", 0666)
	if err == nil {
		err = newMan.SaveTo(f)
	}
	if _ = f.Close(); err != nil {
		_ = newMan.SaveTo(os.Stdout)
		return fmt.Errorf("write manifest error: %w", err)
	}

	success = true

	// delete old file locally as well as in s3.
	c.logger.Printf(`deleting "s3://%s/%s"`, originalManifest.Bucket, originalManifest.Key)
	if _, err = downloadClient.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:              &originalManifest.Bucket,
		Key:                 &originalManifest.Key,
		ExpectedBucketOwner: downloadExpectedBucketOwner,
	}); err != nil {
		c.logger.Printf("delete old S3 file error: %v", err)
	}

	c.logger.Printf(`deleting "%s"`, originalManifestName)
	if err = os.Remove(originalManifestName); err != nil {
		c.logger.Printf("delete old manifest error: %v", err)
	}

	return nil
}
