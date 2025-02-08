# xy3
[![Go Reference](https://pkg.go.dev/badge/github.com/nguyengg/xy3.svg)](https://pkg.go.dev/github.com/nguyengg/xy3)

`xy3` is born out of my need to create S3 backups while using [XYplorer](https://en.wikipedia.org/wiki/XYplorer). Here
are the XYplorer's file associations that I use:
```
|"Stream and extract from S3" s3>"xy3.exe" "download" --stream-and-extract
|"Download from S3" s3>"xy3.exe" "download"
|"Delete from S3" s3>"xy3.exe" "remove"
|"Compress and upload to S3" \>"xy3.exe" "upload" -b "bucket-name" -k "<curfolder>/"
|"Upload to S3" *>"xy3.exe" "upload" -b "bucket-name" -k "<curfolder>/"
```

## Setup

```shell
go get github.com/nguyengg/xy3
```

## CLI

`xy3` exists as a CLI that I use with XYplorer on a daily basis.

```shell
# Uploading a file will generate a local .s3 (JSON) file that stores metadata about how to retrieve the file.
# For example, this command will create doc.txt.s3 and log.zip.s3.
xy3 up -b "bucket-name" -k "key-prefix/" --expected-bucket-owner "1234" doc.txt log.zip

# Downloading from the JSON .s3 files will create unique names to prevent duplicates.
# For example, since doc.txt and log.zip still exist, this command will create doc-1.txt and log-1.zip.
xy3 down doc.txt.s3 log.zip.s3

# To remove both local and remote files, use this command.
xy3 remove doc.txt.s3 log.zip.s3
```

## Go Module

`xy3` can also be used as a Go module. I have a few programs that actually depend on `xy3` for the ability to upload to
and download from S3 with progress bar. Here's an example:

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/schollz/progressbar/v3"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, _ := config.LoadDefaultConfig(ctx)
	client := s3.NewFromConfig(cfg)

	// Upload only accepts name to files on the local filesystem.
	file := "path/to/file"
	stat, _ := os.Stat(file)
	_, _ = xy3.Upload(ctx, client, file, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("my-bucket"),
		Key:    aws.String("my-file"),
	}, func(options *xy3.UploadOptions) {
		// I can change the concurrency (default to 3 goroutines) or put a throttle on the upload.
		options.Concurrency = runtime.NumCPU()
		options.MaxBytesInSecond = 5242880 // 5MiB

		// this example uses a progress bar to show upload progress.
		bar := progressbar.DefaultBytes(stat.Size(), "uploading")

		// PreUploadPart is used to keep track of the size of each part (which should be identical).
		var completedPartCount int32
		parts := make(map[int32]int)
		options.PreUploadPart = func(partNumber int32, data []byte) {
			parts[partNumber] = len(data)
		}

		// PostUploadPart increases the progress bar by the completed size.
		options.PostUploadPart = func(part types.CompletedPart, partCount int32) {
			if completedPartCount++; completedPartCount == partCount {
				_ = bar.Close()
			} else {
				_ = bar.Add64(int64(parts[aws.ToInt32(part.PartNumber)]))
			}
		}
	})

	// Download must be given an io.Writer.
	f, _ := os.CreateTemp("", "*")
	defer f.Close()

	_ = xy3.Download(ctx, client, "my-bucket", "my-file", f, func(options *xy3.DownloadOptions) {
		// similar to upload, I can change the concurrency (default to 3 goroutines) or put a limit.
		options.Concurrency = runtime.NumCPU()
		options.MaxBytesInSecond = 5242880 // 5MiB

		// the size parameter is actually the total file size to be downloaded, which makes it easy to
		// update the progress bar.
		var bar *progressbar.ProgressBar
		var completedPartCount int
		options.PostGetPart = func(data []byte, size int64, partNumber, partCount int) {
			if bar == nil {
				bar = internal.DefaultBytes(size, "downloading")
			}
			if completedPartCount++; completedPartCount == partCount {
				_ = bar.Close()
			} else {
				_ = bar.Add64(int64(len(data)))
			}
		}
	})
}

```

## Zip compress and extract

You can use `github.com/nguyengg/xy3/zipper` directly to ZIP-compress directories and extract them. See 
[zipper](zipper/README.md) for more information.

## S3 Manager

If you want to use [github.com/aws/aws-sdk-go-v2/feature/s3/manager](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager)
that comes with the SDK and adds logging, see [managerlogging](managerlogging/README.md).
