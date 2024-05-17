package remove

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jessevdk/go-flags"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
)

type Command struct {
	KeepLocal bool `long:"keep-local" description:"by default, the local files will be deleted upon successfully deleted in S3; specify this to keep the local files intact"`
	Args      struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files each containing a single S3 URI" required:"yes"`
	} `positional-args:"yes"`

	client *s3.Client
}

func (c *Command) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load default config error:%w", err)
	}

	c.client = s3.NewFromConfig(cfg)

	// to prevent accidental download, prompt for each file.
	prompt := true
	reader := bufio.NewReader(os.Stdin)

	success := 0
	n := len(c.Args.Files)

fileLoop:
	for i, file := range c.Args.Files {
	promptLoop:
		for prompt {
			fmt.Printf("Confirm deletion of \"%s\":\n", file)
			fmt.Printf("\tY/y: to proceed with deletion\n")
			fmt.Printf("\tN/n: to skip this file\n")
			fmt.Printf("\tF/f: to start deleting without prompt for all remaining files including this\n")

			line, err := reader.ReadString('\n')
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Printf("stdin ended; successfully deleted %d/%d files", success, n)
					return nil
				}
				return fmt.Errorf("read prompt error: %w", err)
			}
			switch strings.ToLower(strings.TrimSpace(line)) {
			case "y":
				break promptLoop
			case "n":
				success++
				continue fileLoop
			case "f":
				prompt = false
			}
		}

		if err = c.remove(ctx, string(file)); err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("interrupted; successfully deleted %d/%d files", success, n)
				return nil
			}

			log.Printf("%d/%d: remove %s error: %v", i+1, n, filepath.Base(string(file)), err)
			continue
		}

		success++
	}

	log.Printf("successfully deleted %d/%d files", success, n)
	return nil
}
