# Implements io.ReadSeeker, io.ReaderAt, and io.WriterTo using S3 ranged GetObject

This module provides implementations of `io.ReadSeeker`, `io.ReaderAt`, and `io.WriterTo` for S3 downloading needs.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3/s3writer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	// s3writer.Writer implements io.Writer and io.ReaderFrom so I can start piping local file to upload.
	w, err := s3writer.New(ctx, client, &s3.PutObjectInput{
		Bucket: aws.String("my-bucket"),
		Key:    aws.String("my-key"),
	})
	if err != nil {
		log.Fatal(err)
	}

	// either way below will work.
	f, _ := os.Open("/path/to/file")
	_, err = f.WriteTo(w)
	//_, err = w.ReadFrom(f)
	_ = f.Close()
}

```
