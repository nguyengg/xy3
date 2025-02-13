# Implements io.ReadSeeker, io.ReaderAt, and io.WriterTo using S3 ranged GetObject

[![Go Reference](https://pkg.go.dev/badge/github.com/nguyengg/xy3.svg)](https://pkg.go.dev/github.com/nguyengg/xy3/s3reader)

This module provides implementations of `io.ReadSeeker`, `io.ReaderAt`, and `io.WriterTo` for S3 downloading needs.

```go
package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/krolaw/zipstream"
	"github.com/nguyengg/xy3/s3reader"
	"github.com/nguyengg/xy3/zipper"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	// s3reader.Reader implements both io.ReadSeeker and io.ReaderAt so I can start streaming the
	// S3 object however I want.
	// if in interactive mode, s3reader.WithProgressBar will show a progress bar displaying progress.
	// otherwise, use s3reader.WithProgressLogger instead.
	reader, err := s3reader.New(ctx, client, &s3.GetObjectInput{
		Bucket: aws.String("my-bucket"),
		Key:    aws.String("my-key"),
	}, s3reader.WithProgressBar())
	if err != nil {
		log.Fatal(err)
	}

	// for example, if reader is a ZIP file, I can use xy3 to extract the zip file headers
	// without reading the whole file.
	cd, err := zipper.NewCDScanner(reader, reader.Size())
	if err != nil {
		log.Fatal(err)
	}
	for fh := range cd.All() {
		// fh is a zipper.CDFileHeader which embeds zip.FileHeader.
		// in theory, with the offset, I should be able to use reader.ReadAt to find
		// the local file header and perform parallel decompression on each file in
		// the archive.
		log.Printf("%s can be found at offset=%d", fh.Name, fh.Offset)
	}

	// In this example, zipstream is used instead to stream the entire file.
	// I do need to either reset the reader with Seek, or create a new one using Reopen.
	for zr := zipstream.NewReader(reader.Reopen()); ; {
		fh, err := zr.Next()
		if err != nil {
			log.Fatal(err)
		}

		// zr implements Reader as well so extract the file like this.
		f, err := os.Create(fh.Name)
		if err == nil {
			_, err = io.Copy(f, zr)
			_ = f.Close()
		}
		if err != nil {
			log.Fatal(f)
		}
	}
}

```
