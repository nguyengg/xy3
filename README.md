# xy3

`xy3` is born out of my need to create S3 backups while using [XYplorer](https://en.wikipedia.org/wiki/XYplorer). Here
are the XYplorer's file associations that I use:
```
|"Download from S3" s3>"xy3.exe" "download" -p default
|"Delete from S3" s3>"xy3.exe" "rm" -p default
|"Upload to S3" *>"xy3.exe" "upload" -b "bucket-name" -k "<curfolder>/" -p default
```

The program has three subcommands and can be used as standalone program as well:
* [Upload](#upload)
* [Download](#download)
* [Remove](#remove)

```shell
$ xy3 -h
Usage:
  xy3 [OPTIONS] <download | remove | upload>

Application Options:
  -p, --profile= override AWS_PROFILE if given

Help Options:
  -h, --help     Show this help message

Available commands:
  download  download files from S3 (aliases: down)
  remove    remove both local and S3 files (aliases: rm)
  upload    upload files to S3 (aliases: up)
```

## Upload

```shell
$ xy3 up -h
Usage:
  xy3 [OPTIONS] upload [upload-OPTIONS] file...

Application Options:
  -p, --profile=                   override AWS_PROFILE if given

Help Options:
  -h, --help                       Show this help message

[upload command options]
      -b, --bucket=                name of the S3 bucket containing the
                                   files
      -k, --key-prefix=            key prefix to apply to all S3 operations
          --expected-bucket-owner= optional ExpectedBucketOwner field to
                                   apply to all S3 operations
      -d, --delete                 if given, the local files will be
                                   deleted only if uploaded successfully
      -P, --max-concurrency=       use up to max-concurrency number of
                                   goroutines at a time. If not given,
                                   default to the number of logical CPUs.
                                   (default: 0)

[upload command arguments]
  file:                            the local files to be uploaded to S3
```

## Download

```shell
$ xy3 up -h
Usage:
  xy3 [OPTIONS] download [download-OPTIONS] file...

Application Options:
  -p, --profile=             override AWS_PROFILE if given

Help Options:
  -h, --help                 Show this help message

[download command options]
      -P, --max-concurrency= use up to max-concurrency number of goroutines
                             at a time. If not given, default to the number
                             of logical CPUs. (default: 0)

[download command arguments]
  file:                      the local files each containing a single S3
                             URI
```

## Remove

```shell
$ xy3 rm -h
Usage:
  xy3 [OPTIONS] remove [remove-OPTIONS] file...

Application Options:
  -p, --profile=        override AWS_PROFILE if given

Help Options:
  -h, --help            Show this help message

[remove command options]
          --keep-local  by default, the local files will be deleted upon
                        successfully deleted in S3; specify this to keep
                        the local files intact

[remove command arguments]
  file:                 the local files each containing a single S3 URI
```
