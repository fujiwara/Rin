package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

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
		batchMode   bool
		debug       bool
		dryRun      bool
		maxImports  int
		maxExecTime time.Duration
	)
	flag.StringVar(&config, "config", "config.yaml", "config file path")
	flag.StringVar(&config, "c", "config.yaml", "config file path")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.BoolVar(&debug, "d", false, "enable debug logging")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&batchMode, "batch", false, "batch mode")
	flag.BoolVar(&batchMode, "b", false, "batch mode")
	flag.BoolVar(&dryRun, "dry-run", false, "dry run mode (load configuration only)")
	flag.IntVar(&maxImports, "max-imports", 0, "max number of imports in batch execution")
	flag.DurationVar(&maxExecTime, "max-exec-time", 0, "max execution time duration in batch execution (e.g 60s)")
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

	run := rin.Run
	if dryRun {
		run = rin.DryRun
	}
	opt := rin.Option{
		Batch:        batchMode,
		MaxExecCount: maxImports,
		MaxExecTime:  maxExecTime,
	}
	if err := run(config, &opt); err != nil {
		log.Println("[error]", err)
		os.Exit(1)
	}
}
