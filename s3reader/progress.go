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
//
// Note: WithProgressLogger is useful only if you're using Reader as an io.Reader or io.WriterTo. Any Reader.Seek will
// cause the progress logger to become incorret. there is n. Reader.ReadAt will not provide updates.
func WithProgressLogger(logger *log.Logger, interval time.Duration) func(*Options) {
	return func(opts *Options) {
		opts.logger = &logLogger{
			logger: logger,
			rate:   &rate.Sometimes{Interval: interval},
			size:   opts.size,
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

type progressLogger interface {
	io.WriteCloser
	io.Seeker
	Reopen() progressLogger
}

type logLogger struct {
	logger       *log.Logger
	rate         *rate.Sometimes
	offset, size int64
}

var _ progressLogger = (*logLogger)(nil)

func (l *logLogger) Write(p []byte) (n int, err error) {
	n = len(p)
	l.offset += int64(n)

	l.rate.Do(func() {
		l.logger.Printf("downloaded %s / %s so far", humanize.IBytes(uint64(l.offset)), humanize.IBytes(uint64(l.size)))
	})

	return n, nil
}

func (l *logLogger) Close() error {
	if l.offset == l.size {
		l.logger.Printf("downloaded %s in total", humanize.IBytes(uint64(l.offset)))
	} else {
		l.logger.Printf("downloaded %s / %s in total", humanize.IBytes(uint64(l.offset)), humanize.IBytes(uint64(l.size)))
	}

	return nil
}

func (l *logLogger) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		l.offset = min(l.size-1, max(0, offset))
	case io.SeekCurrent:
		l.offset = min(l.size-1, max(0, l.offset+offset))
	case io.SeekEnd:
		l.offset = min(l.size-1, max(0, l.size+offset))
	}

	return l.offset, nil
}

func (l *logLogger) Reopen() progressLogger {
	return &logLogger{
		logger: l.logger,
		rate:   &rate.Sometimes{Interval: l.rate.Interval},
		offset: 0,
		size:   l.size,
	}
}

type barLogger struct {
	bar          *progressbar.ProgressBar
	opts         []progressbar.Option
	offset, size int64
}

var _ progressLogger = (*barLogger)(nil)

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
	n = len(p)
	b.offset += int64(n)
	return n, nil
}

func (b *barLogger) Close() error {
	if b.bar != nil {
		return b.bar.Close()
	}

	return nil
}

func (b *barLogger) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		offset = min(b.size-1, max(0, offset))
	case io.SeekCurrent:
		offset = min(b.size-1, max(0, b.offset+offset))
	case io.SeekEnd:
		offset = min(b.size-1, max(0, b.size+offset))
	}

	if delta := offset - b.offset; delta != 0 && b.bar != nil {
		_ = b.bar.Add64(delta)
	}

	b.offset = offset
	return b.offset, nil
}

func (b *barLogger) Reopen() progressLogger {
	return &barLogger{
		opts: b.opts,
		size: b.size,
	}
}

type noopLogger struct {
	io.Writer
}

var _ progressLogger = (*noopLogger)(nil)

func (n noopLogger) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

func (n noopLogger) Reopen() progressLogger {
	return n
}

func (n noopLogger) Close() error {
	return nil
}
