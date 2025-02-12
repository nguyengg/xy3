package s3reader

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

// WithProgressLogger adds a progress logger that logs download progress with the given interval.
//
// For example, if interval is `5*time.Second`, every 5 seconds, the given logger will print `downloaded X / Y so far`
// where X is the number of bytes that have been downloaded, Y the total number of expected bytes, both X and Y are
// displayed in a human-friendly format (e.g. 5 KiB, 1 MiB, etc.).
func WithProgressLogger(logger *log.Logger, interval time.Duration) func(*Options) {
	return func(opts *Options) {
		opts.logger = &logLogger{
			logger: logger,
			rate:   &rate.Sometimes{Interval: interval},
			size:   uint64(opts.size),
		}
	}
}

// WithProgressBar adds a progress bar that displays download progress.
//
// You can also use `r.WriteTo(io.MultiWriter(file, bar))` if you're using Reader.WriteTo to write to file.
func WithProgressBar(options ...progressbar.Option) func(*Options) {
	return func(opts *Options) {
		// don't create the progress bar here.
		// create on first write instead.
		opts.logger = &barLogger{opts: options, size: opts.size}
	}
}

// all loggers here are cloneable to make Reader.Reopen works.
type cloneable interface {
	Clone() io.WriteCloser
}

type logLogger struct {
	logger        *log.Logger
	rate          *rate.Sometimes
	written, size uint64
}

func (l *logLogger) Write(p []byte) (n int, err error) {
	n = len(p)
	l.written += uint64(n)

	l.rate.Do(func() {
		l.logger.Printf("downloaded %s / %s so far", humanize.IBytes(l.written), humanize.IBytes(l.size))
	})

	return n, nil
}

func (l *logLogger) Close() error {
	if l.written == l.size {
		l.logger.Printf("downloaded %s in total", humanize.IBytes(l.written))
	} else {
		l.logger.Printf("downloaded %s / %s in total", humanize.IBytes(l.written), humanize.IBytes(l.size))
	}

	return nil
}

func (l *logLogger) Clone() io.WriteCloser {
	return &logLogger{
		logger:  l.logger,
		rate:    &rate.Sometimes{Interval: l.rate.Interval},
		written: 0,
		size:    l.size,
	}
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
			progressbar.OptionSetDescription(fmt.Sprintf("downloading")),
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

func (b *barLogger) Clone() io.WriteCloser {
	return &barLogger{
		opts: b.opts,
		size: b.size,
	}
}

type noopLogger struct {
	io.Writer
}

func (n noopLogger) Close() error {
	return nil
}
