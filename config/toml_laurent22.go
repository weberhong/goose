package config
/*

import (
    "github.com/laurent22/toml-go"
    "fmt"
)

type TomlConf_laurent22 struct {
    conf            toml.Document
}

func (this *TomlConf_laurent22) String(key string) string {
    return this.conf.GetString(key)
}

func (this *TomlConf_laurent22) Int64(key string) int64 {
    return this.conf.GetInt64(key)
}

func (this *TomlConf_laurent22) Float64(key string) float64 {
    return this.conf.GetFloat64(key)
}

func (this *TomlConf_laurent22) Bool(key string) bool {
    return this.conf.GetBool(key)
}

type tomlConfParser_laurent22 struct {
}

func (this *tomlConfParser_laurent22) Parse(file string) (c Conf,err error) {
    defer func() {
        if r := recover();r != nil {
            err = fmt.Errorf("%s",r)
        }
    }()

    tconf := &TomlConf_laurent22{}
    var parser toml.Parser
    tconf.conf = parser.ParseFile(file)

    return tconf,nil
}

func init() {
    //github.com/laurent22/toml-go
    //在语法有错误的情况下会陷入死循环
    //https://github.com/laurent22/toml-go/issues/2
    //使用其它toml解析库替代
    //register(".toml",&tomlConfParser_laurent22{})
}
*/
