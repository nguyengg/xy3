package s3writer

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/time/rate"
)

// WithProgressLogger adds a progress logger that logs upload progress with the given interval.
//
// For example, if interval is `5*time.Second`, every 5 seconds, the given logger will print `uploaded X so far` where
// X is the number of bytes that have been uploaded a human-friendly format (e.g. 5 KiB, 1 MiB, etc.).
func WithProgressLogger(logger *log.Logger, interval time.Duration) func(*Options) {
	return func(opts *Options) {
		opts.logger = &logLogger{
			logger: logger,
			rate:   &rate.Sometimes{Interval: interval},
		}
	}
}

// WithProgressLoggerAndSize is variant of WithProgressLogger with an expected number of bytes.
//
// The log format will become `upload X / Y so far`.
func WithProgressLoggerAndSize(logger *log.Logger, interval time.Duration, size uint64) func(*Options) {
	return func(opts *Options) {
		opts.logger = &sizedLogLogger{
			logger: logger,
			rate:   &rate.Sometimes{Interval: interval},
			size:   size,
		}
	}
}

// WithProgressBar adds a progress bar that displays download progress.
//
// You can also use `w.ReadFrom(io.TeeReader(file, bar))` if you're using Writer.ReadFrom to upload a file. However,
// due to the buffered nature of Writer, the progress bar will be slightly incorrect here since it's reflecting the
// number of bytes read from file, which may be larger than the number of bytes actually uploaded.
//
// WithProgressBar (and WithProgressLogger) guarantees that the progress updates happen **after** each successful part
// upload and thus will show the correct numbers.
func WithProgressBar(size int64, options ...progressbar.Option) func(*Options) {
	return func(opts *Options) {
		// don't create the progress bar here.
		// create on first write instead.
		opts.logger = &barLogger{opts: options, size: size}
	}
}

type logLogger struct {
	logger  *log.Logger
	rate    *rate.Sometimes
	written uint64
}

func (l *logLogger) Write(p []byte) (n int, err error) {
	n = len(p)
	l.written += uint64(n)

	l.rate.Do(func() {
		l.logger.Printf("uploaded %s so far", humanize.IBytes(l.written))
	})

	return n, nil
}

func (l *logLogger) Close() error {
	l.logger.Printf("uploaded %s in total", humanize.IBytes(l.written))

	return nil
}

type sizedLogLogger struct {
	logger        *log.Logger
	rate          *rate.Sometimes
	written, size uint64
}

func (l *sizedLogLogger) Write(p []byte) (n int, err error) {
	n = len(p)
	l.written += uint64(n)

	l.rate.Do(func() {
		l.logger.Printf("uploaded %s / %s so far", humanize.IBytes(l.written), humanize.IBytes(l.size))
	})

	return n, nil
}

func (l *sizedLogLogger) Close() error {
	if l.written == l.size {
		l.logger.Printf("uploaded %s in total", humanize.IBytes(l.written))
	} else {
		l.logger.Printf("uploaded %s / %s in total", humanize.IBytes(l.written), humanize.IBytes(l.size))
	}

	return nil
}

type barLogger struct {
	bar  *progressbar.ProgressBar
	opts []progressbar.Option
	size int64
}

func (b *barLogger) Write(p []byte) (n int, err error) {
	if b.bar == nil {
		// DefaultBytes with higher throttler to reduce flickering.
		b.bar = progressbar.NewOptions64(b.size, append([]progressbar.Option{
			progressbar.OptionSetDescription(fmt.Sprintf("uploading")),
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowTotalBytes(true),
			progressbar.OptionSetWidth(10),
			progressbar.OptionThrottle(1 * time.Second),
			progressbar.OptionShowCount(),
			progressbar.OptionOnCompletion(func() {
				fmt.Fprint(os.Stderr, "\n")
			}),
			progressbar.OptionSpinnerType(14),
			progressbar.OptionFullWidth(),
			progressbar.OptionSetRenderBlankState(true),
		}, b.opts...)...)
	}

	// ignore all errors from progress bar.
	_, _ = b.bar.Write(p)

	return len(p), nil
}

func (b *barLogger) Close() error {
	if b.bar != nil {
		return b.bar.Close()
	}

	return nil
}

type noopLogger struct {
	io.Writer
}

func (n noopLogger) Close() error {
	return nil
}
