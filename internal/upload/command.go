package upload

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jessevdk/go-flags"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
)

type Command struct {
	Bucket              string  `short:"b" long:"bucket" description:"name of the S3 bucket containing the files" required:"true"`
	Prefix              string  `short:"k" long:"key-prefix" description:"key prefix to apply to all S3 operations"`
	ExpectedBucketOwner *string `long:"expected-bucket-owner" description:"optional ExpectedBucketOwner field to apply to all S3 operations"`
	Delete              bool    `short:"d" long:"delete" description:"if given, the local files will be deleted only if uploaded successfully"`
	MaxConcurrency      int     `short:"P" long:"max-concurrency" description:"use up to max-concurrency number of goroutines at a time. If not given, default to the number of logical CPUs." default:"0"`
	Args                struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files to be uploaded to S3"`
	} `positional-args:"yes" required:"yes"`

	client *s3.Client
}

func (c *Command) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	switch {
	case c.MaxConcurrency < 0:
		return fmt.Errorf("max-concurrency cannot be negative")
	case c.MaxConcurrency == 0:
		c.MaxConcurrency = runtime.NumCPU()
		log.Printf("using max concurrency %d (logical CPU count)", c.MaxConcurrency)
	default:
		log.Printf("using max concurrency %d", c.MaxConcurrency)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load default config error:%w", err)
	}

	c.client = s3.NewFromConfig(cfg)

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		if err = c.upload(ctx, string(file)); err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("interrupted; successfully uploaded %d/%d files", success, n)
				return nil
			}

			log.Printf("%d/%d: upload %s error: %v", i+1, n, filepath.Base(string(file)), err)
			continue
		}

		success++
	}

	log.Printf("successfully uploaded %d/%d files", success, n)
	return nil
}
