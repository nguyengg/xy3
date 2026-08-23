# xy3

[![Go Reference](https://pkg.go.dev/badge/github.com/nguyengg/xy3.svg)](https://pkg.go.dev/github.com/nguyengg/xy3)

`xy3` is born out of my need to create S3 backups while using [XYplorer](https://en.wikipedia.org/wiki/XYplorer). Here
are the XYplorer file associations that I use:

```
|"Stream and extract from S3" s3>"xy3.exe" "download" --stream-and-extract
|"Download from S3"           s3>"xy3.exe" "download"
|"Delete from S3"             s3>"xy3.exe" "remove"
|"Compress and upload to S3" \>"xy3.exe" "upload" -b "bucket-name" -k "<curfolder>/"
|"Upload to S3"               *>"xy3.exe" "upload" -b "bucket-name" -k "<curfolder>/"
```

## Install

As a Go module:

```shell
go get github.com/nguyengg/xy3
```

As a CLI, either build from source (see [DEVELOPMENT.md](DEVELOPMENT.md)) or grab a binary from the release page.

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

Run `xy3 --help` for the full command list (`compress` / `c`, `extract` / `x`, `upload` / `up`, `download` / `down`,
`remove` / `rm`).

## Go module to compress and extract Zip

You can use `github.com/nguyengg/xy3/zipper` directly to ZIP-compress directories and extract them. See
[zipper](zipper) for more information.

## Development

The build system is [Task](https://taskfile.dev/) and the toolchain is pinned via [mise](https://mise.jdx.dev/). Run
`task --list` for all targets; `task build` produces both Linux and Windows binaries in one command.

For the full contributor workflow — mise setup, pre-commit / pre-push hooks, lint, test, coverage, vulnerability scan
— see [DEVELOPMENT.md](DEVELOPMENT.md).
