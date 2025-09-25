package xy3

import (
	"archive/tar"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
)

type tarCodec struct {
	wc  io.WriteCloser
	tw  *tar.Writer // nil until NewFile is called at least once.
	dec decompressor
}

// archiver.
var _ archiver = &tarCodec{}

func (tc *tarCodec) AddFile(src, dst string) error {
	dst = filepath.ToSlash(dst)

	if tc.tw == nil {
		tc.tw = tar.NewWriter(tc.wc)
	}

	fi, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf(`stat file "%s" error: %w`, src, err)
	}

	hdr, err := tar.FileInfoHeader(fi, dst)
	if err != nil {
		return fmt.Errorf(`create tar header for "%s" error: %w`, src, err)
	}
	hdr.Name = dst

	if err = tc.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf(`write tar header for "%s" error: %w`, src, err)
	}

	return nil
}

func (tc *tarCodec) Write(p []byte) (int, error) {
	if tc.tw != nil {
		return tc.tw.Write(p)
	}

	return tc.wc.Write(p)
}

func (tc *tarCodec) Close() (err error) {
	if tc.tw != nil {
		if err = tc.tw.Close(); err != nil {
			return fmt.Errorf("close tar writer error: %w", err)
		}
	}

	if err = tc.wc.Close(); err != nil {
		return fmt.Errorf("close archiver error: %w", err)
	}

	return nil
}

// extractor.
var _ extractor = &tarCodec{}

func (tc *tarCodec) Files(src io.Reader, open bool) (iter.Seq2[archiveFile, error], error) {
	r, err := tc.dec.NewDecoder(src)
	if err != nil {
		return nil, err
	}

	return func(yield func(archiveFile, error) bool) {
		for f, err := range untar(r) {
			if !yield(f, err) || err != nil {
				return
			}
		}

		if err = r.Close(); err != nil {
			yield(nil, err)
		}
	}, nil
}

func untar(src io.Reader) iter.Seq2[archiveFile, error] {
	tr := tar.NewReader(src)

	return func(yield func(archiveFile, error) bool) {
		for {
			hdr, err := tr.Next()
			if err != nil {
				if err == io.EOF {
					return
				}

				yield(nil, fmt.Errorf("read next tar entry error: %w", err))
				return
			}

			if hdr.Typeflag == tar.TypeDir {
				// TODO support creating empty directories.
				continue
			}

			if !yield(&tarEntry{hdr: hdr, ReadCloser: io.NopCloser(tr)}, nil) {
				return
			}
		}
	}
}

type tarEntry struct {
	hdr *tar.Header
	io.ReadCloser
}

func (e *tarEntry) Name() string {
	return e.hdr.Name
}

func (e *tarEntry) FileInfo() os.FileInfo {
	return e.hdr.FileInfo()
}

func (e *tarEntry) FileMode() os.FileMode {
	return os.FileMode(e.hdr.Mode)
}
