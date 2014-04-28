package database

import (
    . "github.com/getwe/goose/utils"
    "container/heap"
    "math"
)

// 倒排拉链.实现保证轻量级,可任意拷贝保证不损耗性能
type InvList []Index

// 追加拉链或者元素.
func (l *InvList) Append(i ...Index) {
    *l = append(*l,i...)
}

// 追加一条拉链
func (l *InvList) Concat(lst InvList) {
    *l = append(*l,lst...)
}

// 获取长度
func (l InvList) Len() int{
    return len(l)
}

// 获取容量
func (l InvList) Cap() int{
    return cap(l)
}

func (l InvList) IsFull() bool{
    return l.Len() == l.Cap()
}

// 增加cap.实际是新分配一个slice并copy数据过去
func (l *InvList) IncCap(newCap int) {
    if cap(*l) < newCap {
        t := NewInvList(newCap)
        copy(t,*l)
        *l = t
    }
}

// 归并拉链.假设两拉链都是递增有序,合并后保存有序;最多归并MaxSize个结果
func (l *InvList) Merge(src InvList,MaxSize ...int) {
    maxSz := math.MaxInt32
    if len(MaxSize) > 0 {
        maxSz = MaxSize[0]
    }
    l.merge(src,maxSz)
}
func (l *InvList) merge(src InvList,MaxSize int) {
    dstLen := len(*l) + len(src)
    if dstLen > MaxSize {
        dstLen = MaxSize
    }
    // 预留空间
    list := make([]Index,0,dstLen)
    var i,j int = 0,0
    for ; i < len(*l) && j < len(src) && len(list) < MaxSize; {
        if (*l)[i].InID < src[j].InID {
            list = append(list,(*l)[i])
            i++
        } else if (*l)[i].InID > src[j].InID {
            list = append(list,src[j])
            j++
        } else {
            // 进行InvList Merge的是同一个term的两条拉链,同一个term命中
            // 了同一个doc(内部id),但是出现在两个拉链中,这个逻辑不会发生
            panic("two list of the same termsign have a same doc(in) ? ")
        }

    }

    if len(list) < MaxSize {

        if i < len(*l) {
            toAppendLen := i + ( MaxSize - len(list) )
            if toAppendLen > len(*l) {
                toAppendLen = len(*l)
            }

            list = append(list,(*l)[i:toAppendLen]...)
        }
        if j < len(src) {
            toAppendLen := j + ( MaxSize - len(list) )
            if toAppendLen > len(src) {
                toAppendLen = len(src)
            }

            list = append(list,src[j:toAppendLen]...)
        }
    }

    *l = list
}

// InvList最小堆元素
type InvListMinHeapItem struct {
    index   int
    list    InvList
}
type InvListMinHeap []InvListMinHeapItem

// 堆必须支持接口:Len
func (ih InvListMinHeap) Len() int {
    return len(ih)
}
// 堆排序必须支持接口:Less
func (ih InvListMinHeap) Less(i,j int) bool {
    indexa := ih[i].index
    indexb := ih[j].index
    // InID小的先归并
    return ih[i].list[indexa].InID < ih[j].list[indexb].InID
}
// 堆排序必须支持接口:Swap
func (ih InvListMinHeap) Swap(i,j int) {
    ih[i],ih[j] = ih[j],ih[i]
}
// 堆排序必须支持接口:Push
func (ih *InvListMinHeap) Push(x interface{}) {
    *ih = append(*ih,x.(InvListMinHeapItem))
}
// 堆排序必须支持接口:Pop
func (ih *InvListMinHeap) Pop() interface{} {
    old := *ih
    n := len(old)
    item := old[n-1]
    *ih = old[0:n-1]
    return item
}

// 归并多个有序拉链;最大归并MaxSize个结果.内部实现使用最小堆进行归并
func (l *InvList) KMerge(list [](*InvList),MaxSize ...int) {
    maxSz := math.MaxInt32
    if len(MaxSize) > 0 {
        maxSz = MaxSize[0]
    }
    l.kmerge(list,maxSz)
}
func (l *InvList) kmerge(list [](*InvList),MaxSize int) {

    // 创建最小堆
    listheap := &InvListMinHeap{}
    heap.Init(listheap)

    // 第一个拉链
    if len(*l) > 0 {
        heap.Push(listheap,InvListMinHeapItem{index:0,list:*l})
    }

    // 全部拉链长度
    allCnt := len(*l)

    // 其它拉链
    for _,e := range list {
        if e != nil && len(*e) > 0 {
            heap.Push(listheap,InvListMinHeapItem{index:0,list:*e})
            // 随便计算一下总数量
            allCnt += len(*e)
        }
    }

    if allCnt > MaxSize {
        allCnt = MaxSize
    }
    // 新拉链,预留cap
    retlst := NewInvList(allCnt+1)

    for listheap.Len() > 0 {
        // 弹出堆顶最小元素
        item := heap.Pop(listheap).(InvListMinHeapItem)
        // 加入结果拉链
        retlst.Append( item.list[item.index] )

        // 长度限制
        if len(retlst) >= MaxSize {
            break
        }

        // 把刚归并了一个元素的拉链重新加入堆,后移一个元素
        if item.index+1 < len(item.list) {
            heap.Push(listheap,InvListMinHeapItem{
                index: item.index+1,
                list : item.list})
        }
    }
    *l = retlst
}


// 创建拉链.可选参数1是是否设置cap
func NewInvList(arg ...int)(InvList) {
    if len(arg) > 0 {
        return make([]Index,0,arg[0])
    } else {
        return make([]Index,0)
    }
}



/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
