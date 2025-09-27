//go:build windows

package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"golang.org/x/term"
)

func exit(err error) {
	// need this on window to keep the console open.
	if term.IsTerminal(int(os.Stdin.Fd())) {
		_, _ = fmt.Fprintf(os.Stderr, "Press any key to close console\n")
		r := bufio.NewReader(os.Stdin)
		_, _, _ = r.ReadRune()
	}

	if err != nil && !flags.WroteHelp(err) {
		os.Exit(1)
	}
}
