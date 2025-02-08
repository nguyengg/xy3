package download

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dustin/go-humanize"
	"github.com/krolaw/zipstream"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/namedhash"
	"github.com/schollz/progressbar/v3"
)

var (
	sigCDFH []byte
	sigEOCD []byte
	sep     = regexp.MustCompile(`[\\/]`)
)

func init() {
	sigCDFH = make([]byte, 4)
	binary.LittleEndian.PutUint32(sigCDFH, 0x02014b50)

	sigEOCD = make([]byte, 4)
	binary.LittleEndian.PutUint32(sigEOCD, 0x06054b50)
}

// findEOCD attempts to do a number of ranged GET to parse the central directory of the zip file.
//
// See https://en.wikipedia.org/wiki/ZIP_(file_format)#End_of_central_directory_record_(EOCD).
//
// The presence of this directory is pretty strong indication that this is a ZIP file.
func (c *Command) findEOCD(ctx context.Context, man manifest.Manifest) (bool, string, error) {
	bar := progressbar.NewOptions(
		12,
		progressbar.OptionSetDescription("inspecting zip file"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowTotalBytes(true),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true))
	defer bar.Close()

	// first, HeadObject to give the complete length.
	var size uint64
	if headObjectOutput, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:              &man.Bucket,
		Key:                 &man.Key,
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	}); err != nil {
		return false, "", err
	} else if size = uint64(aws.ToInt64(headObjectOutput.ContentLength)); size <= 10*1024*1024 {
		// if the file is less than 10 MB then don't bother with streaming.
		_ = bar.Close()
		log.Printf("file is too small for streaming")
		return false, "", nil
	}
	_ = bar.Add(1)

	// get the last part of partSize bytes. if the zip has so much comment that the signature doesn't show up in
	// this blob, the file is not eligible for streaming mode.
	partSize := uint64(1024)
	offset := size - partSize
	getObjectInput := &s3.GetObjectInput{
		Bucket:              &man.Bucket,
		Key:                 &man.Key,
		ExpectedBucketOwner: man.ExpectedBucketOwner,
		Range:               aws.String(fmt.Sprintf("bytes=%d-", offset)),
	}
	data, err := get(ctx, c.client, getObjectInput)
	if err != nil {
		return false, "", err
	}

	i := bytes.LastIndex(data, sigEOCD)
	if i == -1 {
		log.Printf("no end of central directory signature in last %d bytes", partSize)
		return false, "", nil
	}

	// now get the entire central directory with all the file headers.
	partSize = uint64(binary.LittleEndian.Uint32(data[i+12 : i+16]))
	offset = uint64(binary.LittleEndian.Uint32(data[i+16 : i+20]))
	if partSize > 5*1024*1204 {
		_ = bar.Close()
		log.Printf("central directory's size (%s) is too large for streaming", humanize.IBytes(partSize))
		return false, "", nil
	}

	getObjectInput.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+partSize-1))
	if data, err = get(ctx, c.client, getObjectInput); err != nil {
		return false, "", err
	}

	// https://en.wikipedia.org/wiki/ZIP_(file_format)#Central_directory_file_header_(CDFH)
	// this is a variant of zipper.findRoot that can work with file headers containing both / or \.
	root := ""
	for len(data) > 0 {
		n, m, k := nmk([6]byte(data[28:34]))
		name := string(data[46 : 46+n])
		paths := sep.Split(name, 2)
		if len(paths) == 1 {
			// this is a file at top level so there is no root for sure.
			return true, "", nil
		}

		switch root {
		case paths[0]:
		case "":
			root = paths[0]
		default:
			return true, "", nil
		}

		data = data[46+n+m+k:]
	}

	return true, root, nil
}

func get(ctx context.Context, client *s3.Client, input *s3.GetObjectInput) ([]byte, error) {
	getObjectOutput, err := client.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	defer getObjectOutput.Body.Close()

	var buf bytes.Buffer
	if _, err = io.Copy(&buf, getObjectOutput.Body); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func nmk(b [6]byte) (n int, m int, k int) {
	return int(b[0]) | int(b[1])<<8, int(b[2]) | int(b[3])<<8, int(b[4]) | int(b[5])<<8
}

func (c *Command) streamAndExtract(ctx context.Context, man manifest.Manifest) (bool, error) {
	// check first if we're eligible for stream and extract mode.
	ok, root, err := c.findEOCD(ctx, man)
	if !ok || err != nil {
		return false, err
	}
	trimRoot := func(path string) string {
		return path
	}
	if root != "" {
		trimRoot = func(path string) string {
			return strings.TrimLeft(strings.TrimPrefix(path, root), `\/`)
		}
	}

	// while downloading, also computes checksum to verify against the downloaded content.
	h, err := namedhash.NewFromChecksumString(man.Checksum)
	if err != nil {
		return true, err
	}

	// attempt to create the local directory that will store the extracted files.
	// if we fail to download the file complete, clean up by deleting the directory.
	stem, _ := xy3.StemAndExt(man.Key)
	dir, err := xy3.MkExclDir(".", stem)
	if err != nil {
		return true, fmt.Errorf("create output directory error: %w", err)
	}

	success := false
	defer func(name string) {
		if !success {
			c.logger.Printf(`deleting output directory "%s"`, name)
			if err = os.RemoveAll(name); err != nil {
				c.logger.Printf("delete output directory error: %v", err)
			}
		}
	}(dir)

	c.logger.Printf(`extracting from "s3://%s/%s" to "%s"`, man.Bucket, man.Key, dir)

	// we'll use a pipe here.
	// one goroutine will be responsible for downloading to PipeWriter.
	// the main goroutine then reads from PipeReader to extract files using krolaw/zipstream. if there is error with
	// reading/extracting the files, cancel the child context so that all goroutines can gracefully stop.
	ctx, cancel := context.WithCancelCause(ctx)
	pr, pw := io.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer pw.Close()

		if err := xy3.Download(ctx, c.client, man.Bucket, man.Key, io.MultiWriter(pw, h), func(options *xy3.DownloadOptions) {
			options.Concurrency = c.MaxConcurrency
			options.ModifyHeadObjectInput = func(input *s3.HeadObjectInput) {
				v := man.ExpectedBucketOwner
				if v == nil {
					v = c.ExpectedBucketOwner
				}
				input.ExpectedBucketOwner = v
			}
			options.ModifyGetObjectInput = func(input *s3.GetObjectInput) {
				v := man.ExpectedBucketOwner
				if v == nil {
					v = c.ExpectedBucketOwner
				}
				input.ExpectedBucketOwner = v
			}

			var bar *progressbar.ProgressBar
			var completedPartCount int
			options.PostGetPart = func(data []byte, size int64, partNumber, partCount int) {
				if bar == nil {
					bar = internal.DefaultBytes(size, "stream and extract")
				}
				if completedPartCount++; completedPartCount == partCount {
					_ = bar.Close()
				} else {
					_ = bar.Add64(int64(len(data)))
				}
			}
		}); err != nil && !errors.Is(err, context.Canceled) {
			cancel(fmt.Errorf("download error: %w", err))
		}
	}()

	var (
		buf = make([]byte, 32*1024)
		fh  *zip.FileHeader
		f   *os.File
	)
	for zr := zipstream.NewReader(pr); err == nil; {
		fh, err = zr.Next()
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			err = fmt.Errorf("stream zip error: %w", err)
			break
		}

		name := fh.Name
		path := filepath.Join(dir, trimRoot(name))

		fi := fh.FileInfo()
		if fi.IsDir() {
			err = os.MkdirAll(path, fi.Mode())
			continue
		}

		if err = os.MkdirAll(filepath.Dir(path), 0755); err == nil {
			f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, fi.Mode())
		}
		if err != nil {
			err = fmt.Errorf(`create file "%s" error: %w`, path, err)
			break
		}

		err = xy3.CopyBufferWithContext(ctx, f, zr, buf)
		_ = f.Close()
		if err != nil {
			err = fmt.Errorf(`write to file "%s" error: %w`, path, err)
			break
		}

		// TODO how to report file writing progress?
	}

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			cancel(err)
		}

		return true, err
	}

	wg.Wait()
	success = true
	return true, nil
}
