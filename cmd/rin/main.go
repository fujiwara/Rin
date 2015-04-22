package main

import (
	"flag"

	rin "github.com/fujiwara/Rin"
)

func main() {
	var (
		config string
	)
	flag.StringVar(&config, "config", "config.yaml", "config file path")
	flag.Parse()
	err := rin.Run(config)
	if err != nil {
		panic(err)
	}
}
