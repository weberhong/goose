package database

import (
	log "github.com/getwe/goose/log"
	. "github.com/getwe/goose/utils"
	"sort"
)

// 最初几次分配空间较小,短拉链就满足了,再往后就固定分配2k个空间
var allocNum = []int{16, 256, 512, 1024, 1024}

func GetDyAllocNum(i int) int {
	if i > 0 && i < len(allocNum) {
		return allocNum[i]
	}
	return 2048
}

// 逻辑大拉链,正排转倒排的过程中使用.
type bigInvList struct {
	AllList [](InvList)
}

// 增加一个索引
func (this *bigInvList) Append(i Index) {
	length := len(this.AllList)
	// 如果还未分配任何拉链,或者最后一个拉链空间已经用完,则分配新拉链
	if length == 0 || this.AllList[length-1].IsFull() {
		this.addlist()
	}

	last := len(this.AllList) - 1
	this.AllList[last].Append(i)
}

func (this *bigInvList) addlist() {
	length := len(this.AllList)
	this.AllList = append(this.AllList, NewInvList(GetDyAllocNum(length)))
}

// IndexTransform 模块完成将正排转成倒排拉链的工作.
//  * 不支持并发
//  * 占用大量内存
type IndexTransform struct {
	// 特点:
	//  * 每条拉链的基本操作是在频繁在末尾append,最终一次性遍历写出
	//  * 每条拉链长度不定,无法预估,有的拉链只有1个doc,也有高频词几十万个doc
	//  * 不能直接使用一个slice,预留长度不好预估,动态分配空间时耗cpu
	// 实现思路一:
	// 一次性分配indexCount大小的slice,逻辑上按indexBlockCount为单位分块
	// 每次按块为单位分配给一个term使用
	//  * 一次性分配内存,减少cpu压力
	//  * indexBlockCount大小不好预估,对于短拉链,块内其它空间浪费
	//
	// 实现思路二:
	// 每个term采用多次分配多个slice,每次分配长度成倍数增加,相当于就是手动管理
	// 多个slice组成一个大的逻辑slice,手动是避免一个slice在cap不够的情况下出现
	// 整个slice进行一次拷贝.对于倒排,并不需要要求内存上连续

	bighash map[TermSign]*bigInvList

	// 已经保存term数量
	termInDocCount int

	// 支持最大term数量
	// 一个IndexTransform占用多少内存,假设2000w个term,一个term8个字节
	// 2000*10000 * 8 = 153MB
	// 再加上map其他内存占用,在200MB以下
	// BUG(honggengwei)实际测试发现占用内存严重大于该估算,需要进一步测试验证
	maxTermInDocCount int
}

// 拉链数量,也就是总共的term数量
func (this *IndexTransform) GetInvListSize() int {
	return len(this.bighash)
}

// 第一个返回值bool类型,false表示还可以继续写入;true表示不可以再增加,需要Dump.
func (this *IndexTransform) isFull() bool {
	if this.termInDocCount >= this.maxTermInDocCount {
		return true
	}
	return false
}

// 增加一个doc. id是doc的内部id. termList是doc中切词得到的全部term.
func (this *IndexTransform) AddOneDoc(id InIdType, termList []TermInDoc) error {
	for _, t := range termList {
		err := this.addOneTerm(id, t)
		if err != nil {
			return err
		}
		this.termInDocCount++
	}
	return nil
}

func (this *IndexTransform) addOneTerm(id InIdType, t TermInDoc) error {
	list, ok := this.bighash[t.Sign]
	if !ok {
		// 不存在,创建新拉链
		this.bighash[t.Sign] = new(bigInvList)
	}
	list, ok = this.bighash[t.Sign]
	if !ok {
		// 还取不到
		return log.Error("add new value fail")
	}

	list.Append(Index{InID: id, Weight: t.Weight})
	return nil
}

// 将已经构建好的索引全部写入索引库中.
func (this *IndexTransform) Dump(db WriteOnlyIndex) error {
	var termlist TermSignSlice = make([]TermSign, 0, len(this.bighash))
	for k, _ := range this.bighash {
		termlist = append(termlist, k)
	}

	// 对term进行排序
	sort.Sort(termlist)

	// 遍历全部term
	for _, t := range termlist {
		list, ok := this.bighash[t]
		if !ok {
			continue
		}
		// 写入
		allLen := 0
		for _, l := range list.AllList {
			allLen += l.Len()
		}
		// TODO 分配了内存,把原来的结果最新拷贝一遍,整个实现中的缺陷
		newlst := NewInvList(allLen)
		for _, l := range list.AllList {
			newlst.Concat(l)
		}

		err := db.WriteIndex(t, &newlst)
		if err != nil {
			return err
		}
	}

	termlist = nil
	this.bighash = nil
	return nil
}

// IndexTransform构造函数
func NewIndexTransform(MaxTermCnt int) *IndexTransform {
	t := IndexTransform{}

	t.termInDocCount = 0
	t.maxTermInDocCount = MaxTermCnt
	t.bighash = make(map[TermSign]*bigInvList)

	return &t
}
