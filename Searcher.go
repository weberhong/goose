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

func (this *Searcher) Search(context *StyContext, reqbuf []byte, resbuf []byte) (reslen int, err error) {

	// 解析请求
	termInQList, queryInfo, err := this.strategy.ParseQuery(reqbuf, context)
	if err != nil {
		return 0, err
	}

	// 构建查询树
	me, err := NewMergeEngine(this.db, termInQList)
	if err != nil {
		return 0, err
	}

	result := make([]SearchResult, 0, GOOSE_DEFAULT_SEARCH_RESULT_CAPACITY)

	// term命中doc的情况
	termInDocList := make([]TermInDoc, len(termInQList))
	var allfinish bool = false

	for allfinish != true {
		var inId InIdType
		var currValid bool

		inId, currValid, allfinish = me.Next(termInDocList)
		if currValid != true {
			continue
		}

		outId, err := this.db.GetOutID(inId)
		if err != nil {
			context.Log.Warn("GetOutId fail [%s] InId[%d] OutId[%d]", err, inId, outId)
			continue
		}

		if inId == 0 || outId == 0 {
			context.Log.Warn("MergeEngine get illeagl doc InId[%d] OutId[%d]", inId, outId)
			continue
		}

		weight, err := this.strategy.CalWeight(queryInfo, inId, outId,
			termInQList, termInDocList, uint32(len(termInQList)), context)
		if err != nil {
			context.Log.Warn("CalWeight fail %s", err)
			continue
		}

		result = append(result, SearchResult{
			InId:   inId,
			OutId:  outId,
			Weight: weight})

	}

	// 完成
	reslen, err = this.strategy.Response(queryInfo, result, this.db, this.db, resbuf, context)
	if err != nil {
	}

	return reslen, nil
}

func NewSearcher(db DataBaseReader, sty SearchStrategy) (*Searcher, error) {
	var s Searcher
	s.db = db
	s.strategy = sty
	return &s, nil
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
