package database

import (
    . "github.com/getwe/goose/utils"
    "sync"
    "sort"
)

// 内存索引.并发模式支持多读一写.
type MemoryIndex struct {
    // 读写锁
    rwlock      sync.RWMutex

    // 内存倒排
    ri    map[TermSign] *InvList
}

// 读取索引,每次查询在内部分配一块内存返回
func (this *MemoryIndex) ReadIndex(t TermSign)(*InvList,error) {
    this.rwlock.RLock()
    defer this.rwlock.RUnlock()

    tmp,ok := this.ri[t]
    if !ok {
        l := NewInvList()
        return &l,nil
    }

    // NewInvList(cap)
    l := NewInvList(tmp.Len())
    l.Concat(*tmp)
    return &l,nil
}

// 写入索引,内部加锁进行写入.
// 同一个term多次写入,会进行append操作.
func (this *MemoryIndex) WriteIndex(t TermSign,l *InvList) (error) {
    this.rwlock.Lock()
    this.rwlock.Unlock()

    tmp,ok := this.ri[t]
    if !ok {
        *tmp = NewInvList()
    }

    tmp.Concat(*l)

    this.ri[t] = tmp

    return nil
}

// 关闭所有索引文件
func (this *MemoryIndex) Close() {
    this.Clear()
}

func (this *MemoryIndex) Clear() {
    this.rwlock.Lock()
    this.rwlock.Unlock()

    this.ri = make(map[TermSign]*InvList)
}

// 内存库中有多少条拉链
func (this *MemoryIndex) GetTermCount() int64 {
    return int64(len(this.ri))
}

// MemoryIndex构造函数
func NewMemoryIndex() (*MemoryIndex) {
    index := MemoryIndex{}
    index.ri = make(map[TermSign]*InvList)
    return &index
}

// 内存索引遍历器
type MemoryIndexIterator struct {
    currTermCnt     int
    termArray       TermSignSlice
}

// 获取下一个term,遍历结束返回0
//
// 
func (this *MemoryIndexIterator) Next() (TermSign) {
    this.currTermCnt++
    if this.currTermCnt >= len(this.termArray){
        return TermSign(0)
    }
    return this.termArray[this.currTermCnt]
}

// 需要注意:
// 创建新的迭代器.在返回的迭代器生命有效期间,MemoryIndex必须有效.
// 迭代器能够访问到的term列表是执行这次的快照.
// NewIterator返回后再写入MemoryIndex的拉链MemoryIndexIterator不一定能够访问到.
// 因此在MemoryIndexIterator遍历整个内存库的时候,应该暂停写入,但是可以读取.
// 暂停写入需要外部调用者自行控制.
func (this *MemoryIndex) NewIterator() (IndexIterator) {
    i := MemoryIndexIterator{}
    i.currTermCnt = -1

    {
        // 获取term快照期间,如果有写入操作是有危险性的,因此加锁
        this.rwlock.Lock()

        // 预留多一个空间,保证i.termArray不会重新分配空间
        i.termArray = make([]TermSign,0,len(this.ri) + 1)
        for k,_ := range this.ri {
            i.termArray = append(i.termArray,k)
        }

        this.rwlock.Unlock()
    }

    sort.Sort(i.termArray)

    // 返回的迭代器实现了IndexIterator.Next接口.所以可以返回
    // 实际类型是*MemoryIndexIterator,后续在其它地方的复制都是复制指针
    return &i
}

