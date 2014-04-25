package goose

import (
	. "github.com/getwe/goose/database"
	. "github.com/getwe/goose/utils"
)

type Searcher struct {
    // 只读数据库
    db DataBaseReader

    // 检索策略逻辑
    strategy SearchStrategy
}

func (this *Searcher) Search(context *StyContext,reqbuf []byte,resbuf []byte) (err error) {
    where := "Searcher.Search"

    // 解析请求
    termInQList,queryInfo,err := this.strategy.ParseQuery(reqbuf,context)
    if err != nil {
        return NewGooseError(where,"parsequery fail",err.Error())
    }

    // 构建查询树
    me,err := NewMergeEngine(this.db,termInQList)
    if err != nil {
        return NewGooseError(where,err.Error(),"")
    }

    result := make([]SearchResult,GOOSE_DEFAULT_SEARCH_RESULT_CAPACITY)

    termInDocList := make([]TermInDoc,len(termInQList))
    for {
        inId,currValid,allfinish := me.Next(termInDocList); 
        if currValid != true {
            continue
        }

        outId,err := this.db.GetOutID(inId)
        if err != nil {
            continue
        }

        weight,err := this.strategy.CalWeight(queryInfo,inId,outId,
            termInQList,termInDocList,uint32(len(termInQList)),context)
        if err != nil {
            continue
        }

        result = append(result,SearchResult{
            InId : inId,
            OutId : outId,
            Weight : weight})

        if allfinish {
            break
        }
    }

    // 结果过滤
    err = this.strategy.Filt(queryInfo,result,context)
    if err != nil {
    }

    // 调权
    err = this.strategy.Adjust(queryInfo,result,this.db,context)
    if err != nil {
    }

    // 完成
    err = this.strategy.Response(queryInfo,result,this.db,resbuf,context)
    if err != nil {
    }

    return nil
}




func NewSearcher(db DataBaseReader,sty SearchStrategy) (*Searcher,error) {
    var s Searcher
    s.db = db
    s.strategy = sty
    return &s,nil
}



/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
