package archive

import (
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/bodgit/sevenzip"
)

// SevenZip implements Archiver.Open for 7z files.
type SevenZip struct {
}

var _ Archiver = SevenZip{}

func (s SevenZip) Create(_ io.Writer, _ string) (AddFunction, CloseFunction, error) {
	panic("not implemented")
}

func (s SevenZip) Open(src io.Reader) (iter.Seq2[File, error], error) {
	f, ok := src.(*os.File)
	if !ok {
		return nil, fmt.Errorf("7z archives must be opened as os.File")
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf(`stat file "%s" error: %w`, f.Name(), err)
	}

	zr, err := sevenzip.NewReader(f, fi.Size())
	if err != nil {
		return nil, fmt.Errorf(`open 7z file "%s" error: %w`, f.Name(), err)
	}

	return func(yield func(File, error) bool) {
		for _, zf := range zr.File {
			if !yield(&sevenZipFile{
				FileHeader: zf.FileHeader,
				open:       zf.Open,
			}, nil) {
				return
			}
		}
	}, nil
}

func (s SevenZip) ArchiveExt() string {
	return "7z"
}

func (s SevenZip) ContentType() string {
	return "application/x-7z-compressed"
}

type sevenZipFile struct {
	sevenzip.FileHeader
	open func() (io.ReadCloser, error)
}

var _ File = &sevenZipFile{}

func (f *sevenZipFile) Name() string {
	return f.FileHeader.Name
}

func (f *sevenZipFile) Open() (io.ReadCloser, error) {
	return f.open()
}
