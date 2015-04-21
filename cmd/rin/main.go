package main

import (
	"flag"

	rin "github.com/fujiwara/Rin"
)

func main() {
	var (
		config string
		port   int
	)
	flag.StringVar(&config, "config", "config.yaml", "config file path")
	flag.IntVar(&port, "port", 3000, "listen port")
	flag.Parse()
	err := rin.Run(config, port)
	if err != nil {
		panic(err)
	}
}
