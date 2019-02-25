package main

import (
	"database/sql"
	"log"

	"github.com/kshvakov/clickhouse"
	"github.com/alecthomas/kingpin"
	"github.com/r3nic1e/chcleaner"
)

var (
	rulesPath = kingpin.Flag("rules", "Path to rules file").Default("rules.yml").Envar("RULES_PATH").ExistingFile()
	dbAddr = kingpin.Flag("db", "Clickhouse address").Default("tcp://127.0.0.1:9000").Envar("CLICKHOUSE_ADDR").URL()
	runServer = kingpin.Command("server", "Run server")
)

func main() {
	_ = kingpin.Parse()
	chcleaner.ReadConfig(*rulesPath)
	connect, err := sql.Open("clickhouse", (*dbAddr).String())
	if err != nil {
		log.Fatal(err)
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Println(err)
		}
		return
	}

	for _, cleaner := range chcleaner.Cleaners {
		cleaner.Run(connect)
	}
}
