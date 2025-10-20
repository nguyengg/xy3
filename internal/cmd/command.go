package cmd

import (
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal/cmd/download"
	"github.com/nguyengg/xy3/internal/cmd/upload"
)

type Xy3 struct {
	Compress Compress         `command:"compress" alias:"c" description:"compress files"`
	Extract  Extract          `command:"extract" alias:"x" description:"extract archives"`
	Download download.Command `command:"download" alias:"down" description:"download from S3"`
	Upload   upload.Command   `command:"upload" alias:"up" description:"upload files to S3"`
	Remove   Remove           `command:"remove" alias:"rm" description:"remove both local and S3 files"`
}

func NewParser() (*flags.Parser, error) {
	opts := &Xy3{}

	p := flags.NewNamedParser("xy3", flags.Default)
	if _, err := p.AddGroup("Global Options", "", opts); err != nil {
		return nil, err
	}

	return p, nil
}
