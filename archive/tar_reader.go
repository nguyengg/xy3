package archive

import (
	"archive/tar"
	"io"
	"iter"
	"os"
)

// Tar implements Archive for tar archives.
type Tar struct {
}

var _ Archiver = Tar{}

func (t Tar) Open(src io.Reader) (iter.Seq2[File, error], error) {
	tr := tar.NewReader(src)

	return func(yield func(File, error) bool) {
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				return
			}
			if !yield(&tarFile{
				Reader: tr,
				Header: hdr,
			}, err) || err != nil {
				return
			}
		}
	}, nil
}

type tarFile struct {
	*tar.Reader
	*tar.Header
}

var _ File = &tarFile{}

func (f *tarFile) Name() string {
	return f.Header.Name
}

func (f *tarFile) Mode() os.FileMode {
	return os.FileMode(f.Header.Mode)
}

func (f *tarFile) Open() (io.ReadCloser, error) {
	return io.NopCloser(f), nil
}
