package config

import (
    "github.com/laurent22/toml-go"
    "fmt"
)

type TomlConf struct {
    conf            toml.Document
}

func (this *TomlConf) String(key string) string {
    return this.conf.GetString(key)
}

func (this *TomlConf) Int64(key string) int64 {
    return this.conf.GetInt64(key)
}

func (this *TomlConf) Float64(key string) float64 {
    return this.conf.GetFloat64(key)
}

func (this *TomlConf) Bool(key string) bool {
    return this.conf.GetBool(key)
}

type tomlConfParser struct {
}

func (this *tomlConfParser) Parse(file string) (c Conf,err error) {
    defer func() {
        if r := recover();r != nil {
            err = fmt.Errorf("%s",r)
        }
    }()

    tconf := &TomlConf{}
    var parser toml.Parser
    tconf.conf = parser.ParseFile(file)

    return tconf,nil
}

func init() {
    register(".toml",&tomlConfParser{})
}
