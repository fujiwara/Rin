package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
	flag.VisitAll(func(f *flag.Flag) {
		if len(f.Name) <= 1 {
			return
		}
		if s := os.Getenv(strings.ToUpper("RIN_" + f.Name)); s != "" {
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

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	run := rin.RunWithContext
	if dryRun {
		run = rin.DryRunWithContext
	}
	if err := run(ctx, config, batchMode); err != nil {
		log.Println("[error]", err)
		os.Exit(1)
	}
}
