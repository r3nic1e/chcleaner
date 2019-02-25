package chcleaner

import (
	"os"
	"log"
	"regexp"

	"gopkg.in/yaml.v2"
)

var config struct {
	Rules []*cleanerConfig
}

type cleanerConfig struct {
	Databases       []string
	databasesRegexp []*regexp.Regexp
	Tables          []string
	tablesRegexp    []*regexp.Regexp
	Keep            int
}

func (c *cleanerConfig) Compile() {
	c.databasesRegexp = make([]*regexp.Regexp, 0, len(c.Databases))
	for _, s := range c.Databases {
		r, err := regexp.Compile(s)
		if err != nil {
			log.Panic(err)
		}
		c.databasesRegexp = append(c.databasesRegexp, r)
	}

	c.tablesRegexp = make([]*regexp.Regexp, 0, len(c.Tables))
	for _, s := range c.Tables {
		r, err := regexp.Compile(s)
		if err != nil {
			log.Panic(err)
		}
		c.tablesRegexp = append(c.tablesRegexp, r)
	}
}

func ReadConfig(path string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	err = yaml.NewDecoder(f).Decode(&config)
	if err != nil {
		log.Fatal(err)
	}

	Cleaners = make([]*Cleaner, len(config.Rules))
	for i, rule := range config.Rules {
		rule.Compile()
		Cleaners[i] = NewCleaner(rule)
	}
}
