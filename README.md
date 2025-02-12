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

## Go module

This module implements a lot of useful libraries for dealing with S3 io operations.

### Implements io.ReadSeeker, io.ReaderAt, and io.WriterTo using S3 ranged GetObject

See [s3reader](s3reader/README.md).

### Implements io.Writer and io.ReaderFrom to upload to S3

See [s3writer](s3writer/README.md). 

### Zip compress and extract

You can use `github.com/nguyengg/xy3/zipper` directly to ZIP-compress directories and extract them. See 
[zipper](zipper/README.md) for more information.
