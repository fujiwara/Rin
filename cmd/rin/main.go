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
	)
	flag.StringVar(&config, "config", "config.yaml", "config file path")
	flag.StringVar(&config, "c", "config.yaml", "config file path")
	flag.BoolVar(&rin.Debug, "debug", false, "enable debug logging")
	flag.BoolVar(&rin.Debug, "d", false, "enable debug logging")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.Parse()

	if showVersion {
		fmt.Println("version:", version)
		fmt.Println("build:", buildDate)
		return
	}

	err := rin.Run(config)
	if err != nil {
		panic(err)
	}
}
