package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/config"
)

type Remove struct {
	KeepLocal bool `long:"keep-local" description:"by default, the local files will be deleted upon successfully deleted in S3; specify this to keep the local files intact"`
	Args      struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files each containing a single S3 URI" required:"yes"`
	} `positional-args:"yes"`
}

func (c *Remove) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	if _, err := config.Load(ctx); err != nil {
		return fmt.Errorf("load config error: %w", err)
	}

	// to prevent accidental download, prompt for each file.
	prompt := true
	reader := bufio.NewReader(os.Stdin)

	success := 0
	n := len(c.Args.Files)

fileLoop:
	for i, file := range c.Args.Files {
	promptLoop:
		for prompt {
			fmt.Printf("Confirm deletion of \"%s\":\n", file)
			fmt.Printf("\tY/y: to proceed with deletion\n")
			fmt.Printf("\tN/n: to skip this file\n")
			fmt.Printf("\tF/f: to start deleting without prompt for all remaining files including this\n")

			line, err := reader.ReadString('\n')
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Printf("stdin ended; successfully deleted %d/%d files", success, n)
					return nil
				}
				return fmt.Errorf("read prompt error: %w", err)
			}
			switch strings.ToLower(strings.TrimSpace(line)) {
			case "y":
				break promptLoop
			case "n":
				success++
				continue fileLoop
			case "f":
				prompt = false
			}
		}

		if err := c.remove(ctx, string(file)); err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("interrupted; successfully deleted %d/%d files", success, n)
				return nil
			}

			log.Printf("%d/%d: remove %s error: %v", i+1, n, filepath.Base(string(file)), err)
			continue
		}

		success++
	}

	log.Printf("successfully deleted %d/%d files", success, n)
	return nil
}

func (c *Remove) remove(ctx context.Context, name string) error {
	basename := filepath.Base(name)
	logger := log.New(os.Stderr, `"`+basename+`" `, log.LstdFlags)

	man, err := internal.LoadManifestFromFile(name)
	if err != nil {
		return err
	}

	cfg := config.ForBucket(man.Bucket)
	client, err := internal.NewS3ClientFromProfile(ctx, cfg.AWSProfile)
	if err != nil {
		return fmt.Errorf("create s3 client error: %w", err)
	}
	expectedBucketOwner := internal.FirstNonNil(man.ExpectedBucketOwner, cfg.ExpectedBucketOwner)

	// headObject first just in case.
	if _, err = client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:              &man.Bucket,
		Key:                 &man.Bucket,
		ExpectedBucketOwner: expectedBucketOwner,
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}

		var re *http.ResponseError
		if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
			logger.Printf("s3 file no longer exists")
			return nil
		}

		logger.Printf("check s3 object metadata error: %v", err)
	}

	logger.Printf(`deleting "s3://%s/%s"`, man.Bucket, man.Key)

	if _, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: expectedBucketOwner,
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}

		var re *http.ResponseError
		if errors.As(err, &re) && re.HTTPStatusCode() != 404 {
			return fmt.Errorf("remove S3 object error: %w", err)
		}
	}

	if !c.KeepLocal {
		if err = os.Remove(name); err != nil {
			logger.Printf(`deleting file "%s"`, name)
			if err = os.Remove(name); err != nil {
				logger.Printf("remove file error: %v", err)
			}
		}
	}

	return nil
}
