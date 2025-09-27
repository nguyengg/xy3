package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal/cmd"
)

func main() {
	log.SetFlags(0)

	p, err := cmd.NewParser()
	if err == nil {
		_, err = p.Parse()
	}
	if err != nil && !flags.WroteHelp(err) {
		_, _ = fmt.Fprintf(os.Stderr, "%v", err)
	}

	exit(err)
}
