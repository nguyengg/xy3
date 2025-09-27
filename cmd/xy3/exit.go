//go:build !windows

package main

import (
	"os"

	"github.com/jessevdk/go-flags"
)

func exit(err error) {
	if err != nil && !flags.WroteHelp(err) {
		os.Exit(1)
	}
}
