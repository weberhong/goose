package strategysimple

import (
    . "github.com/getwe/goose/utils"
    "github.com/laurent22/toml-go/toml"
    "github.com/getwe/scws4go"
)

// 建库的时候,goose框架建静态库读取文件认为一行是一个doc,动态库一个网络请求是一
// 个doc,这是框架设计.StyIndexer每一个doc是一个json结构,只关注其中4个字段:
//  title:doc的标题,建立索引的字段
//  docid:唯一的外部标志符
//  value:需要作为Value的数据
//  data:需要作为Data的数据
// 这构成了一个最简单的检索元素.
type StyIndexer struct {
    // 共用切词工具
    scws    *scws4go.Scws
}


// 分析一个doc,返回其中的term列表,Value,Data.(必须保证框架可并发调用ParseDoc)
func (this *StyIndexer) ParseDoc(doc interface{}) (OutIdType,[]TermInDoc,*Value,*Data,error) {
    // ParseDoc的功能实现需要注意的是,这个函数是可并发的,使用StyIndexer.*需要注意安全



    // 从doc中提取需要写入Value的数据

    // 从doc中提取需要写入Data的数据

}

// 调用一次初始化
func (this *StyIndexer) Init(conf toml.Document) (error) {

    // scws初始化
    scwsDictPath := conf.GetString("Strategy.Indexer.Scws.xdbdict")
    scwsRulePath := conf.GetString("Strategy.Indexer.Scws.rules")
    scwsForkCnt  := conf.GetInt("Strategy.Indexer.Scws.forkCount")
    this.scws = scws4go.NewScws()
    this.scws.SetDict(scwsDictPath, scws4go.SCWS_XDICT_XDB|scws4go.SCWS_XDICT_MEM)
    this.scws.SetRule(scwsRulePath)
    this.scws.SetCharset("utf8")
    this.scws.SetIgnore(1)
    this.scws.SetMulti(scws4go.SCWS_MULTI_SHORT & scws4go.SCWS_MULTI_DUALITY & scws4go.SCWS_MULTI_ZMAIN)
    this.scws.Init(scwsForkCnt)

    return nil
}

