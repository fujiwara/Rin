package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	rin "github.com/fujiwara/Rin"
	"github.com/hashicorp/logutils"
)

var (
	version   string
	buildDate string
)

func main() {
	var (
		config      string
		showVersion bool
		debug       bool
		dryRun      bool
	)
	opt := &rin.Option{}
	flag.StringVar(&config, "config", "config.yaml", "config file path")
	flag.StringVar(&config, "c", "config.yaml", "config file path")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.BoolVar(&debug, "d", false, "enable debug logging")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&opt.BatchMode, "batch", false, "batch mode")
	flag.BoolVar(&opt.BatchMode, "b", false, "batch mode")
	flag.DurationVar(&opt.MaxExecutionTime, "max-execution-time", 0, "max execution time")
	flag.BoolVar(&dryRun, "dry-run", false, "dry run mode (load configuration only)")
	flag.VisitAll(func(f *flag.Flag) {
		if len(f.Name) <= 1 {
			return
		}
		envName := strings.ToUpper("RIN_" + strings.Replace(f.Name, "-", "_", -1))
		if s := os.Getenv(envName); s != "" {
			f.Value.Set(s)
		}
	})
	flag.Parse()

	if showVersion {
		fmt.Println("version:", version)
		fmt.Println("build:", buildDate)
		return
	}

	minLevel := "info"
	if debug {
		minLevel = "debug"
	}
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "info", "warn", "error"},
		MinLevel: logutils.LogLevel(minLevel),
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)
	log.Println("[info] rin version:", version)
	log.Println("[info] option:", opt.String())

	run := rin.Run
	if dryRun {
		run = rin.DryRun
	}
	if err := run(config, opt); err != nil {
		log.Println("[error]", err)
		os.Exit(1)
	}
}
