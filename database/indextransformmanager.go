package database

import (
	"container/heap"
	"fmt"
	. "github.com/getwe/goose/utils"
	"math"
	"os"
	"path/filepath"
	//"runtime"
)

// IndexTransform的管理器.当IndexTransform无法再写入的时候,进行写入到磁盘操作.
// 当全部工作完成后,进行把已写入磁盘的索引合并成一个大索引.
type IndexTransformManager struct {
	// 磁盘存储目录
	filePath string

	// 磁盘索引临时目录
	tmpDiskPath string

	// 已经暂存到磁盘的索引名称
	diskIndexName []string

	// 已经写入的term数量.由于分多个IndexTransform,term会有重合,统计结果比实际偏大
	termCount int64

	// 每个IndexTransform生命期间最多写入term数量
	maxTermInDocCount int
	// 当前可用的IndexTransform
	currIndexTf *IndexTransform
}

func (this *IndexTransformManager) GetTermCount() int64 {
	return this.termCount
}

// MaxTermCnt指的是在内存中最多的索引数(非拉链数).
// fPath是工作目录
func (this *IndexTransformManager) Init(fPath string, MaxTermCnt int) error {

	this.maxTermInDocCount = MaxTermCnt
	this.filePath = fPath
	this.tmpDiskPath = filepath.Join(this.filePath, "_tf_tmp")

	this.termCount = 0

	return nil
}

// 写入索引.不支持并发写入,调用者需要自己控制.
func (this *IndexTransformManager) WriteIndex(InID InIdType, termlist []TermInDoc) error {
	err := this.checkTransform()
	if err != nil {
		return err
	}
	this.termCount += int64(len(termlist))
	return this.currIndexTf.AddOneDoc(InID, termlist)
}

// 更新内部状态,确认有最新可以使用的IndexTransform
func (this *IndexTransformManager) checkTransform() error {
	// 如果还没分配过
	if this.currIndexTf == nil {
		this.currIndexTf = NewIndexTransform(this.maxTermInDocCount)
		return nil
	}

	// 当前使用的IndexTf内存已经满了,进行写入磁盘操作
	if this.currIndexTf.isFull() {
		err := this.saveTransform()
		if err != nil {
			return err
		}
		this.currIndexTf = NewIndexTransform(this.maxTermInDocCount)
		return nil
	}
	return nil
}

// 把当前还在内存中的IndexTransfor写入磁盘
func (this *IndexTransformManager) saveTransform() error {
	if this.currIndexTf == nil {
		return nil
	}

	db, err := this.newDiskIndex()
	if err != nil {
		return nil
	}
	// IndexTransform.Dump在内存进行大数组排序然后写磁盘操作,整个过程是一个耗时
	// 的阻塞操作.IndexTransform如果并发起来,又可能会造成内存占用过大的问题.
	err = this.currIndexTf.Dump(db)
	if err != nil {
		return nil
	}
	db.Close()
	this.currIndexTf = nil
	return nil
}

func (this *IndexTransformManager) newDiskIndex() (*DiskIndex, error) {
	var maxFileSz uint32 = 1024 * 1024 * 1024 // 1024MB

	// 校验临时目录是否已经存在
	if _, err := os.Stat(this.tmpDiskPath); os.IsNotExist(err) {
		// 不存在
		err := os.Mkdir(this.tmpDiskPath, 0755)
		if err != nil {
			return nil, err
		}
	}

	index := NewDiskIndex()
	name := fmt.Sprintf("indextransform%d", len(this.diskIndexName))
	this.diskIndexName = append(this.diskIndexName, name)
	path := this.tmpDiskPath
	err := index.Init(path, name, maxFileSz, int64(this.currIndexTf.GetInvListSize()))
	if err != nil {
		return nil, err
	}
	return index, nil
}

// 结束写入索引,把已暂存磁盘的全部索引以及在内存中的索引进行合并成大的索引
func (this *IndexTransformManager) Dump(dstdb WriteOnlyIndex) error {

	// 如果len(this.diskIndexName) == 0,可以优化,直接将内存中的正排写入
	// dstdb即可.也就是说如果IndexTransfor开辟的内存够大,就可以实现内存正排直接
	// 写入磁盘,提高效率.
	if len(this.diskIndexName) == 0 && this.currIndexTf != nil {
		err := this.currIndexTf.Dump(dstdb)
		if err != nil {
			return nil
		}
		this.currIndexTf = nil
		return nil
	}

	// 先把内存中的索引写入磁盘
	err := this.saveTransform()
	if err != nil {
		return err
	}

	// diskIndexName保持着全部磁盘索引的磁盘路径,接下来需要全部打开,进行一次
	// 多路归并

	// 创建初始化最小磁盘索引堆
	indexheap := &diskIndexMinHeap{}
	heap.Init(indexheap)

	// 打开全部磁盘索引
	dblist := make([](*DiskIndex), 0, len(this.diskIndexName))
	for _, name := range this.diskIndexName {
		db := NewDiskIndex()
		err := db.Open(this.tmpDiskPath, name)
		if err != nil {
			return err
		}
		// 磁盘索引
		dblist = append(dblist, db)

		// 创建一个迭代器
		iter := db.NewIterator()

		// 第一个term
		term := iter.Next()

		// 加入最小堆
		heap.Push(indexheap, diskIndexMinHeapItem{
			Term:  term,
			Index: db,
			Iter:  iter})
	}

	// 最小堆pop出来的上一个term
	var lastTerm TermSign = TermSign(0)
	// 已经弹出的未写入的term
	lastItemLst := make([]diskIndexMinHeapItem, 0)

	for indexheap.Len() > 0 {
		// 堆顶term最小元素
		item := heap.Pop(indexheap).(diskIndexMinHeapItem)
		// 多个索引中会存在相同的term,得收集起来合并后再写入

		if item.Term != lastTerm && len(lastItemLst) > 0 {
			// 把上一个term未写入的先写入目标索引库
			alllist := make([](*InvList), len(lastItemLst))
			for i, e := range lastItemLst {
				alllist[i], err = e.Index.ReadIndex(e.Term)
				if err != nil {
					// TODO warnning log
					alllist[i] = nil
				}
			}
			tmplst := NewInvList()
			tmplst.KMerge(alllist, math.MaxInt32)
			err := dstdb.WriteIndex(lastTerm, &tmplst)
			if err != nil {
				return err
			}

			// 开始新的term,清空状态
			lastItemLst = lastItemLst[:0]
		} else {
			// nothing
		}
		lastTerm = item.Term

		// 无论是新term还是跟上一个一样的term,都是加入待写入列表
		lastItemLst = append(lastItemLst, item)

		// 检查当前索引是否还有更多的term,再push进堆
		newterm := item.Iter.Next()
		if newterm != 0 {
			heap.Push(indexheap, diskIndexMinHeapItem{
				Term:  newterm,
				Index: item.Index,
				Iter:  item.Iter})
		} else {
			// 索引已经遍历结束的尽早关闭
			item.Index.Close()
		}
	}

	// 删除磁盘临时索引
	err = os.RemoveAll(this.tmpDiskPath)
	if err != nil {
		return err
	}

	return nil
}

// IndexTransformManager构造函数.
// 如果内存中的索引数超过了,就会先把全部索引暂时写入磁盘,等最终再合并全部索引.
func NewIndexTransformManager() *IndexTransformManager {
	tfMgr := IndexTransformManager{}

	tfMgr.diskIndexName = nil
	tfMgr.currIndexTf = nil
	return &tfMgr
}
