# Zip compress and extract

## Setup

```shell
go get github.com/nguyengg/xy3
```

## Code Example

```go
package main

import (
	"context"
	"os"

	"github.com/nguyengg/xy3/zipper"
)

func main() {
	// to compress directory "path/to/dir", I must first create the archive file and open for writing.
	archive, _ := os.CreateTemp("", "*.zip")
	defer os.Remove(archive.Name())

	_ = zipper.CompressDir(context.TODO(), "path/to/dir", archive, func(options *zipper.CompressDirOptions) {
		zipper.WithBestCompression(&options.CompressOptions)

		// If "path/to/dir" looks like this:
		//	path/to/dir/test/a.txt
		//	path/to/dir/test/path/b.txt
		//	path/to/dir/test/another/path/c.txt
		//
		// With UnwrapRoot=false (default), archive.zip looks like this:
		//	test/a.txt
		//	test/path/b.txt
		//	test/another/path/c.txt
		//
		// With UnwrapRoot=true (default), archive.zip looks like this:
		//	a.txt
		//	path/b.txt
		//	another/path/c.txt
		//
		// If I'm using xy3 to both compress and extract, generally it's safe to turn UnwrapRoot on because
		// extract will automatically unwrap root for me.
		options.UnwrapRoot = true
	})
	_ = archive.Close()

	// this is how I'd extract the archive in full.
	// the method returns the actual output directory that was created/used so that if there was an error, I can
	// delete output directory to clean up artifacts.
	dir, _ := zipper.Extract(context.TODO(), archive.Name(), ".", func(options *zipper.ExtractOptions) {
		// Generally I want to leave this false so that Extract will create a new directory for me to prevent
		// conflicts. Extract will use the name of the archive ("archive.zip") to create directory such as
		// archive, archive-1, archive-2, etc.
		//
		// If I don't want Extract to create a new directory, pass false here in which case the dir argument
		// must point to a valid directory (Extract can take care of creating this directory for me as well).
		options.UseGivenDirectory = false

		// It is best to leave this setting off (default) as well. The setting is applicable only if the archive
		// actually has a single common root directory like:
		//	test/a.txt
		//	test/path/b.txt
		//	test/another/path/c.txt
		//
		// If that's the case, the output directory would look like this with NoUnwrapRoot=false:
		//	./archive/a.txt
		//	./archive/path/b.txt
		//	./archive/another/path/c.txt
		//
		// If "./archive" already exists, Extract would have used "./archive-1", "./archive-2", etc.
		//
		// If I pass NoUnwrapRoot=true, the output directory would look like this:
		//	./archive/test/a.txt
		//	./archive/test/path/b.txt
		//	./archive/test/another/path/c.txt
		//
		// If I pass both NoUnwrapRoot=true and UseGivenDirectory=true, the output directory would look like:
		//	./test/a.txt
		//	./test/path/b.txt
		//	./test/another/path/c.txt
		//
		// And finally, if I pass NoUnwrapRoot=false and UseGivenDirectory=true, the output directory becomes:
		//	./a.txt
		//	./path/b.txt
		//	./another/path/c.txt
		options.NoUnwrapRoot = false
	})
}

```
