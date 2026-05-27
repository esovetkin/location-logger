package main

import (
	"fmt"
	"os"

	"github.com/esovetkin/location-logger/internal/cli"
)

func main() {
	if err := cli.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
