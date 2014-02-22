package goose

import (
	. "github.com/getwe/goose/database"
	//. "github.com/getwe/goose/utils"
)

type Searcher struct {
    // 只读数据库
    db DataBaseReader

    // 检索策略逻辑
    strategy SearchStrategy
}

func (s *Searcher) Search(request []byte) (response []byte,err error) {
    /*where := "Searcher.Search"

    // 解析请求
    termInQList,queryInfo,err := s.strategy.ParseQuery(request)
    if err != nil {
        return nil,NewGooseError(where,"parsequery fail",err.Error())
    }
    */

    // 构建查询树


    return nil,nil
}




func CreateSearcher(db DataBaseReader,sty SearchStrategy) (*Searcher,error) {
    var s Searcher
    s.db = db
    s.strategy = sty
    return &s,nil
}



/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
