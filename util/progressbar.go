package util

import (
	"io"
	"os"

	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/schollz/progressbar/v3"
)

func FileProgressBar(f *os.File, description string, options ...progressbar.Option) io.WriteCloser {
	var size int64 = -1

	if fi, err := f.Stat(); err == nil {
		size = fi.Size()
	}

	return tspb.DefaultBytes(size, description, options...)
}
