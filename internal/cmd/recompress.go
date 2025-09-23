package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

type Recompress struct {
	MoveTo string `value-name:"s3://bucket/prefix" long:"move-to" description:"if present, the new archive will be uploaded to this S3 bucket and key prefix instead"`
	Args   struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files each containing a single S3 URI" required:"yes"`
	} `positional-args:"yes"`

	client *s3.Client
	logger *log.Logger
}

func (c *Recompress) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	// if MoveTo is specified, parse and validate them first.
	var bucket, prefix string
	if c.MoveTo != "" {
		if bucket, prefix, err = internal.ParseS3URI(c.MoveTo); err != nil {
			return fmt.Errorf("invalid MoveTo: %w", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load default config error:%w", err)
	}

	c.client = s3.NewFromConfig(cfg, func(options *s3.Options) {
		// without this, getting a bunch of WARN message below:
		// WARN Response has no supported checksum. Not validating response payload.
		options.DisableLogOutputChecksumValidationSkipped = true
	})

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)
		c.logger.Printf("start recompressing")

		if err = c.recompress(ctx, string(file), bucket, prefix); err == nil {
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

func (c *Recompress) recompress(ctx context.Context, originalManifestName, moveToBucket, moveToPrefix string) error {
	algorithm := internal.AlgorithmZstd

	originalManifest, err := manifest.UnmarshalFromFile(originalManifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	// we'll create a temp directory to store all intermediate artifacts.
	// this temp directory is deleted only after complete success.
	stem, ext := util.StemAndExt(originalManifest.Key)

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
	f, err := os.CreateTemp(dir, stem+"-*"+ext)
	if err != nil {
		return fmt.Errorf("create original file error: %w", err)
	}

	if err, _ = internal.Download(ctx, c.client, originalManifest.Bucket, originalManifest.Key, f), f.Close(); err != nil {
		if errors.Is(err, internal.ErrChecksumMismatch{}) {
			c.logger.Print(err)
		} else {
			return fmt.Errorf("download error: %w", err)
		}
	}

	// extract to a new directory inside the working directory.
	uncompressedDir, err := internal.Decompress(ctx, f.Name(), dir)
	if err != nil {
		return fmt.Errorf("decompress error: %w", err)
	}

	// now compress the extracted contents.
	f, err = os.OpenFile(filepath.Join(dir, stem+".tar"+algorithm.Ext()), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("create recompressed file error: %w", err)
	}

	if err, _ = internal.CompressDir(ctx, uncompressedDir, f), f.Close(); err != nil {
		return fmt.Errorf(`compress "%s" error: %w`, filepath.Join(dir, "tmp"), err)
	}

	// reopen the file to upload.
	f, err = os.Open(f.Name())
	if err != nil {
		return fmt.Errorf("open recompressed file error: %w", err)
	}

	// bucket and key might be new values if we're moving to a different location.
	bucket := originalManifest.Bucket
	key := path.Dir(originalManifest.Key) + stem + ".tar" + algorithm.Ext()
	if moveToBucket != "" {
		bucket = moveToBucket
		key = moveToPrefix + stem + ".tar" + algorithm.Ext()
	}

	newMan, err := internal.Upload(ctx, c.client, f, bucket, key, func(opts *internal.UploadOptions) {
		opts.PutObjectInputOptions = func(input *s3.PutObjectInput) {
			input.ContentType = aws.String(internal.DefaultAlgorithm.ContentType())
		}
	})
	_ = f.Close()
	if err != nil {
		return fmt.Errorf("upload error: %w", err)
	}

	// write manifest to a unique local .s3 file.
	f, err = util.OpenExclFile(".", stem, ".tar"+algorithm.Ext()+".s3", 0666)
	if err == nil {
		err = newMan.MarshalTo(f)
	}
	if _ = f.Close(); err != nil {
		_ = newMan.MarshalTo(os.Stdout)
		return fmt.Errorf("write manifest error: %w", err)
	}

	success = true

	// delete old file locally as well as in s3.
	c.logger.Printf(`deleting "s3://%s/%s"`, originalManifest.Bucket, originalManifest.Key)
	if _, err = c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &originalManifest.Bucket,
		Key:    &originalManifest.Key,
	}); err != nil {
		c.logger.Printf("delete old S3 file error: %v", err)
	}

	c.logger.Printf(`deleting "%s"`, originalManifestName)
	if err = os.Remove(originalManifestName); err != nil {
		c.logger.Printf("delete old manifest error: %v", err)
	}

	return nil
}
