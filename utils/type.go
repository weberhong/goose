package utils

import (
	"fmt"
)

const (
	GOOSE_MAX_INVLIST_SIZE = 10 * 10000
	GOOSE_MAX_QUERY_TERM   = 32

	GOOSE_DEFAULT_SEARCH_RESULT_CAPACITY = 10000
)

// 在整个检索系统中,都把term转换为64位签名使用
type TermSign int64

type TermSignSlice []TermSign

// 支持sort包的排序
func (s TermSignSlice) Len() int           { return len(s) }
func (s TermSignSlice) Less(i, j int) bool { return s[i] < s[j] }
func (s TermSignSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// 在整个检索系统中,最多32位表示权重的信息
type TermWeight int32

// 内部id类型
type InIdType uint32

// 外部id类型
type OutIdType uint32

// 索引结构
// InID : 在索引库里面的内部ID,每个外部doc分配一个唯一的InID
// Weight : term在doc中的打分情况
type Index struct {
	InID   InIdType
	Weight TermWeight
}

// 全文数据
type Data []byte

// NewData(length,capacity)
func NewData(arg ...int) Data {
	var v Data
	if len(arg) == 0 {
		v = make([]byte, 0)
	} else if len(arg) == 1 {
		v = make([]byte, arg[0])
	} else {
		v = make([]byte, arg[0], arg[1])
	}
	return v
}

func (this *Data) Len() int {
	return len(*this)
}

// value数据
type Value []byte

// NewValue(length,capacity)
func NewValue(arg ...int) Value {
	var v Value
	if len(arg) == 0 {
		v = make([]byte, 0)
	} else if len(arg) == 1 {
		v = make([]byte, arg[0])
	} else {
		v = make([]byte, arg[0], arg[1])
	}
	return v
}

// term在doc中的信息
type TermInDoc struct {
	// 不存储原始串,存储签名
	Sign TermSign
	// term在doc中的打分,TermWeight在策略中可以自由定制
	Weight TermWeight
}

type TermInQuery struct {
	// 不存储原始串,存储签名
	Sign TermSign

	// term在query中的打分,TermWeight在策略中可以自由定制
	Weight TermWeight

	// term在query中的属性信息,在策略中可以自由定制存储
	Attr uint32

	// 是否是可省词
	CanOmit bool

	// 是否忽略位置信息
	SkipOffset bool
}

// 一个检索结果
type SearchResult struct {
	InId   InIdType
	OutId  OutIdType
	Weight TermWeight
}

// 结果拉链
type SearchResultList []SearchResult

// 支持sort包的排序
func (s SearchResultList) Len() int { return len(s) }
func (s SearchResultList) Less(i, j int) bool {
	if s[i].Weight > s[j].Weight {
		return true
	} else if s[i].Weight < s[j].Weight {
		return false
	}
	return s[i].InId < s[j].InId
}
func (s SearchResultList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

//GooseError : 简单的错误日志
type GooseError struct {
	where  string
	errmsg string
	addmsg string
}

func (e *GooseError) Error() string {
	return fmt.Sprintf("[%s|%s]%s", e.where, e.errmsg, e.addmsg)
}
func NewGooseError(w string, e string, a string) *GooseError {
	ge := &GooseError{}
	ge.where = w
	ge.errmsg = e
	ge.addmsg = a
	return ge
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
