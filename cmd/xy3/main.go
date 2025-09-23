package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"

	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal/cmd"
)

func main() {
	log.SetFlags(0)

	// change window's title to cwd.
	if runtime.GOOS == "windows" {
		if dir, err := os.Getwd(); err == nil {
			_ = exec.Command("title", dir).Run()
		}
	}

	p, err := cmd.NewParser()
	if err == nil {
		_, err = p.Parse()
	}
	if err != nil && !flags.WroteHelp(err) {
		_, _ = fmt.Fprintf(os.Stderr, "%v", err)
	}

	// need this on window to keep the console open.
	if runtime.GOOS == "windows" {
		_, _ = fmt.Fprintf(os.Stderr, "Press any key to close console\n")
		_, _ = fmt.Scanf("h")
	}

	if err != nil && !flags.WroteHelp(err) {
		os.Exit(1)
	}
}
