package main

import (
	"flag"

	rin "github.comf/fujiwara/Rin"
)

func main() {
	var (
		config string
		port   int
	)
	flag.StringVar(&config, "config", "config file path")
	flag.IntVar(&port, "port", "listen port")
	flag.Parse()
	err := rin.Run(config, port)
	if err != nil {
		panic(err)
	}
}
