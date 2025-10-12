package archive

import (
	"archive/tar"
	"io"
	"iter"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/nguyengg/xy3/codec"
	"github.com/nguyengg/xy3/internal"
)

// Tar implements Archiver for tar archives.
type Tar struct {
	// Codec if given will be used to encode/decode contents with Archiver.Open or Archiver.Create.
	codec.Codec
}

var _ Archiver = &Tar{}

func (t *Tar) Create(dst io.Writer, root string) (add AddFunction, closer CloseFunction, err error) {
	root = filepath.ToSlash(root)

	var enc io.WriteCloser

	if t.Codec != nil {
		enc, err = t.Codec.NewEncoder(dst)
		if err != nil {
			return
		}
	} else {
		enc = &internal.WriteNoopCloser{Writer: dst}
	}

	w := tar.NewWriter(enc)

	add = func(name string, fi os.FileInfo) (io.WriteCloser, error) {
		name = filepath.ToSlash(name)
		isDir := fi.IsDir() || strings.HasSuffix(name, "/")

		hdr, err := tar.FileInfoHeader(fi, name)
		if err != nil {
			return nil, err
		}

		if isDir {
			hdr.Name = path.Join(root, name) + "/"
		} else {
			hdr.Name = path.Join(root, name)
		}

		if err = w.WriteHeader(hdr); err != nil {
			return nil, err
		}

		return &internal.WriteNoopCloser{Writer: w}, nil
	}

	closer = internal.ChainCloser(w.Close, enc.Close)

	return
}

func (t *Tar) Open(src io.Reader) (_ iter.Seq2[File, error], err error) {
	var dec io.ReadCloser

	if t.Codec != nil {
		if dec, err = t.Codec.NewDecoder(src); err != nil {
			return
		}
	} else {
		dec = io.NopCloser(src)
	}

	tr := tar.NewReader(dec)

	return func(yield func(File, error) bool) {
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}

			if !yield(&tarFile{
				Reader: tr,
				Header: hdr,
			}, err) || err != nil {
				return
			}
		}

		if err = dec.Close(); err != nil {
			yield(nil, err)
		}
	}, nil
}

func (t *Tar) ArchiveExt() string {
	if t.Codec != nil {
		return ".tar" + t.Codec.Ext()
	}

	return ".tar"
}

func (t *Tar) ContentType() string {
	if t.Codec != nil {
		return t.Codec.ContentType()
	}

	return "application/x-tar"
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
