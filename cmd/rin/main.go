package main

import (
	"flag"
	"fmt"

	rin "github.com/fujiwara/Rin"
)

var (
	version   string
	buildDate string
)

func main() {
	var (
		config      string
		showVersion bool
		batchMode   bool
	)
	flag.StringVar(&config, "config", "config.yaml", "config file path")
	flag.StringVar(&config, "c", "config.yaml", "config file path")
	flag.BoolVar(&rin.Debug, "debug", false, "enable debug logging")
	flag.BoolVar(&rin.Debug, "d", false, "enable debug logging")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&batchMode, "batch", false, "batch mode")
	flag.BoolVar(&batchMode, "b", false, "batch mode")
	flag.Parse()

	if showVersion {
		fmt.Println("version:", version)
		fmt.Println("build:", buildDate)
		return
	}

	err := rin.Run(config, batchMode)
	if err != nil {
		panic(err)
	}
}
