package cmd

import (
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal/cmd/awsconfig"
	"github.com/nguyengg/xy3/internal/cmd/download"
	"github.com/nguyengg/xy3/internal/cmd/upload"
)

type Xy3 struct {
	Profile    string           `short:"p" long:"profile" description:"if given, all AWS operations will use this shared profile" value-name:"aws-profile"`
	Compress   Compress         `command:"compress" alias:"c" description:"compress files"`
	Extract    Extract          `command:"extract" alias:"x" description:"extract archives"`
	Recompress Recompress       `command:"recompress" description:"recompress archives"`
	Download   download.Command `command:"download" alias:"down" description:"download from S3"`
	Upload     upload.Command   `command:"upload" alias:"up" description:"upload files to S3"`
	Remove     Remove           `command:"remove" alias:"rm" description:"remove both local and S3 files"`
}

func NewParser() (*flags.Parser, error) {
	opts := &Xy3{}

	p := flags.NewNamedParser("xy3", flags.Default)
	if _, err := p.AddGroup("Global Options", "", opts); err != nil {
		return nil, err
	}

	p.CommandHandler = func(command flags.Commander, args []string) error {
		if opts.Profile != "" {
			if c, ok := command.(awsconfig.ConfigLoader); ok {
				c.AddOption(config.WithSharedConfigProfile(opts.Profile))
			}
		}

		return command.Execute(args)
	}

	return p, nil
}
