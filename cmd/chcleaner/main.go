package main

import (
	"runtime"
	"io"
	"os"
	"strings"

	"github.com/alecthomas/kingpin"
	"github.com/r3nic1e/chcleaner"
)

var (
	rulesPath = kingpin.Flag("rules-file", "Path to rules file").Envar("RULES_PATH").ExistingFile()
	rules = kingpin.Flag("rules", "Rules").Envar("RULES").String()
	dbAddr = kingpin.Flag("db", "Clickhouse address").Default("tcp://127.0.0.1:9000").Envar("CLICKHOUSE_ADDR").URL()
	socketAddr = kingpin.Flag("socket", "Socket address to bind to").Default("tcp://0.0.0.0:8000").Envar("SOCKET_ADDR").URL()
	runServer = kingpin.Command("cron", "Run daemon")
	runOnce = kingpin.Command("run", "Run once")
)

func main() {
	kingpin.CommandLine.HelpFlag.Short('h')
	switch kingpin.Parse() {
	case "cron":
		rulesReader := openRules()
		chcleaner.ReadConfig(rulesReader, (*dbAddr).String())

		for _, cleaner := range chcleaner.Cleaners {
			cleaner.Start()
		}

		for {
			runtime.Gosched()
		}
	case "run":
		rulesReader := openRules()
		chcleaner.ReadConfig(rulesReader, (*dbAddr).String())

		for _, cleaner := range chcleaner.Cleaners {
			cleaner.Run()
		}
	}
}

func openRules() io.Reader {
	if *rulesPath != "" {
		if f, err := os.Open(*rulesPath); err == nil {
			return f
		}
	}

	return strings.NewReader(*rules)
}
