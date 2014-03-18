package goose

import (
    . "github.com/getwe/goose/utils"
    . "github.com/getwe/goose/database"
    "github.com/laurent22/toml-go"
)

// 建索引策略.
// 框架会调用一次Init接口进行初始化,建索引的时候会N个goroutine调用ParseDoc
type IndexStrategy interface {
    // 全局初始化的接口
    Init(conf toml.Document) (error)

    // 分析一个doc,返回其中的term列表,Value,Data
    ParseDoc(doc interface{}) (OutIdType,[]TermInDoc,*Value,*Data,error)
}

type SearchStrategy interface {
    // 全局初始化的接口
    Init(conf toml.Document) (error)

    // 解析请求
    // 返回term列表,一个由策略决定的任意数据,后续接口都会透传
    ParseQuery(request []byte)([]TermInQuery,interface{},error)

    // 对一个结果进行打分,确定相关性
    // queryInfo    : ParseQuery策略返回的结构
    // inId         : 需要打分的doc的内部id
    // outId        : 需求打分的doc的外部id
    // termInQuery  : 所有term在query中的打分
    // termInDoc    : 所有term在doc中的打分
    // termCnt      : term数量
    // Weight       : 返回doc的相关性得分
    // 返回错误当前结果则丢弃
    // @NOTE query中的term不一定能命中doc,TermInDoc.Weight == 0表示这种情况
    CalWeight(queryInfo interface{},inId InIdType,outId OutIdType,
        termInQuery []TermInQuery,termInDoc []TermInDoc,
        termCnt uint32) (TermWeight,error)

    // 对结果拉链进行过滤
    Filt(queryInfo interface{},list SearchResultList) (error)

    // 结果调权
    // 确认最终结果列表排序
    Adjust(queryInfo interface{},list SearchResultList,db ValueReader) (error)

    // 构建返回包
    Response(queryInfo interface{},list SearchResultList,
        db DataBaseReader,response []byte) (err error)

}









/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
