package config

import (
	"fmt"
	"path/filepath"
)

type Conf interface {
	String(key string) string
	Int64(key string) int64
	Float64(key string) float64
	Bool(key string) bool
	Float64Array(key string) []float64
}

type confParser interface {
	Parse(file string) (Conf, error)
}

func NewConf(file string) (Conf, error) {
	suffix := filepath.Ext(file)
	p, ok := parsers[suffix]
	if !ok {
		return nil, fmt.Errorf("can't parse file [%s]", file)
	}
	return p.Parse(file)
}

var parsers = make(map[string]confParser)

func register(name string, parser confParser) {
	_, ok := parsers[name]
	if ok {
		panic("duplicate parser")
	}
	parsers[name] = parser
}
