package main

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal/download"
	"github.com/nguyengg/xy3/internal/remove"
	"github.com/nguyengg/xy3/internal/upload"
	"os"
	"runtime"
)

var opts struct {
	Profile  string           `short:"p" long:"profile" description:"override AWS_PROFILE if given" default:"nguyen-gg" default-mask:"-"`
	Download download.Command `command:"download" alias:"down" description:"download files from S3"`
	Upload   upload.Command   `command:"upload" alias:"up" description:"upload files or directories (after compressing the directories with zip) to S3"`
	Remove   remove.Command   `command:"remove" alias:"rm" description:"remove both local and S3 files"`
}

func main() {
	p := flags.NewParser(&opts, flags.Default)
	p.CommandHandler = func(command flags.Commander, args []string) error {
		if opts.Profile != "" {
			if err := os.Setenv("AWS_PROFILE", opts.Profile); err != nil {
				return fmt.Errorf("set AWS_PROFILE error: %w", err)
			}
		}

		return command.Execute(args)
	}

	_, err := p.Parse()

	// need this on window to keep the console open.
	if runtime.GOOS == "windows" {
		_, _ = fmt.Fprintf(os.Stderr, "Press any key to close console\n")
		_, _ = fmt.Scanf("h")
	}

	if err != nil && !flags.WroteHelp(err) {
		os.Exit(1)
	}
}
