package chcleaner

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
)

type table struct {
	database, name string
	partitions     []string
}

func (t table) dropPartition(connect *sql.DB, part string) error {
	sql := fmt.Sprintf("ALTER TABLE %s.%s DROP PARTITION %s", t.database, t.name, part)
	//_, err := connect.Exec(sql)
	//return err
	log.Println(sql)
	return nil
}

func getAllPartitions(connect *sql.DB) []table {
	rows, err := connect.Query("SELECT database, table, groupArray(partition) FROM system.parts WHERE active AND database != 'system' GROUP BY database, table")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var tables []table

	for rows.Next() {
		var t table
		if err := rows.Scan(&t.database, &t.name, &t.partitions); err != nil {
			log.Fatal(err)
		}
		tables = append(tables, t)
	}
	return tables
}

type Cleaner struct {
	config *cleanerConfig
}

var Cleaners []*Cleaner

func NewCleaner(config *cleanerConfig) *Cleaner {
	return &Cleaner{config: config}
}

func (c *Cleaner) checkDatabase(t table) bool {
	for _, regex := range c.config.databasesRegexp {
		log.Println(fmt.Sprintf("Matching %s to %s", t.database, regex))
		if regex.MatchString(t.database) {
			return true
		}
	}
	return false
}

func (c *Cleaner) checkTable(t table) bool {
	for _, regex := range c.config.tablesRegexp {
		if regex.MatchString(t.name) {
			return true
		}
	}
	return false
}

func (c *Cleaner) check(t table) bool {
	return c.checkDatabase(t) && c.checkTable(t)
}

func (c *Cleaner) getPartitionsToDrop(t table) []string {
	sort.Strings(t.partitions)
	if len(t.partitions) <= c.config.Keep {
		return []string{}
	}
	last_index := len(t.partitions) - c.config.Keep
	return t.partitions[0:last_index]
}

func (c *Cleaner) Run(connect *sql.DB) error {
	log.Println(fmt.Sprintf("Running cleaner for %v database, %v table", c.config.Databases, c.config.Tables))
	tables := getAllPartitions(connect)
	for _, t := range tables {
		if !c.check(t) {
			continue
		}

		log.Println(fmt.Sprintf("Table %s fits", t))

		for _, part := range c.getPartitionsToDrop(t) {
			if err := t.dropPartition(connect, part); err != nil {
				log.Print(err)
			}
		}
	}
	return nil
}
