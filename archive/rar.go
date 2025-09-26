package archive

import (
	"io"
	"iter"
	"os"
	"time"

	"github.com/nwaples/rardecode"
)

// Rar implements Archiver for RAR files.
type Rar struct {
}

var _ Archiver = Rar{}

func (r Rar) Create(_ io.Writer, _ string) (AddFunction, CloseFunction, error) {
	panic("not implemented")
}

func (r Rar) Open(src io.Reader) (iter.Seq2[File, error], error) {
	if f, ok := src.(*os.File); ok {
		if rr, err := rardecode.OpenReader(f.Name(), ""); err == nil {
			return fromRarReader(&rr.Reader, rr.Close), nil
		}
	}

	rr, err := rardecode.NewReader(src, "")
	if err != nil {
		return nil, err
	}

	return fromRarReader(rr, func() error {
		return nil
	}), nil
}

func fromRarReader(r *rardecode.Reader, closer func() error) iter.Seq2[File, error] {
	return func(yield func(File, error) bool) {
		for {
			fh, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				yield(nil, err)
				_ = closer() // don't report error from closing
				return
			}

			if !yield(&rarFile{
				rarFileInfo: rarFileInfo{fh},
				Reader:      r,
			}, nil) {
				_ = closer() // don't report error from closing
				return
			}
		}

		if err := closer(); err != nil {
			yield(nil, err)
		}
	}
}

func (r Rar) ArchiveExt() string {
	return ".rar"
}

func (r Rar) ContentType() string {
	return "application/vnd.rar"
}

type rarFile struct {
	rarFileInfo
	io.Reader
}

var _ File = &rarFile{}

func (f *rarFile) FileInfo() os.FileInfo {
	return f
}

func (f *rarFile) Open() (io.ReadCloser, error) {
	return io.NopCloser(f), nil
}

type rarFileInfo struct {
	*rardecode.FileHeader
}

var _ os.FileInfo = &rarFileInfo{}

func (fi *rarFileInfo) Name() string {
	return fi.FileHeader.Name
}

func (fi *rarFileInfo) Size() int64 {
	return fi.FileHeader.UnPackedSize
}

func (fi *rarFileInfo) ModTime() time.Time {
	return fi.FileHeader.ModificationTime
}

func (fi *rarFileInfo) IsDir() bool {
	return fi.FileHeader.IsDir
}

func (fi *rarFileInfo) Sys() any {
	return nil
}
