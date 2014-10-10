package config

/*

import (
    "github.com/kolo/toml.go"
    "fmt"
)

type TomlConf_kolo struct {
    conf    *toml.Conf
}

func (this *TomlConf_kolo) String(key string) string {
    return this.conf.String(key)
}

func (this *TomlConf_kolo) Int64(key string) int64 {
    return this.conf.Int(key)
}

func (this *TomlConf_kolo) Float64(key string) float64 {
    if v := this.conf.Get(key); v != nil {
        return v.(float64)
    } else {
        var zero float64
        return zero
    }
}

func (this *TomlConf_kolo) Bool(key string) bool {
    return this.conf.Bool(key)
}

type tomlConfParser_kolo struct {
}

func (this *tomlConfParser_kolo) Parse(file string) (c Conf,err error) {
    defer func() {
        if r := recover();r != nil {
            err = fmt.Errorf("%s",r)
        }
    }()

    tconf := &TomlConf_kolo{}
    tconf.conf, err = toml.Parse(file)
    if err != nil {
        return nil,err
    }
    return tconf,nil
}

func init() {
    // github.com/kolo/toml.go 模块名字取的不好
    // go get github.com/kolo/toml.go 经常失败
    // 原因见
    // https://groups.google.com/d/msg/golang-nuts/dnOK9j5Fvn4/_WoJt4IekoYJ
    // 使用其它toml解析库替代
    register(".toml",&tomlConfParser_kolo{})
}
*/
