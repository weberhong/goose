package database

import (
    . "github.com/getwe/goose/utils"
    "sync"
    "time"
    "path/filepath"
    log "github.com/getwe/goose/log"
)

type VarIndexStatus struct {
    // 当前使用哪个磁盘索引
    CurrDisk    int
}

// VarIndex利用内存索引和磁盘索引组成.
// 支持检索的时候进行插入索引操作.
type VarIndex struct {
    JsonStatusFile

    // 读操作读写锁,切库的时候先暂停索引读取.
    readLock       sync.RWMutex

    // 写操作互斥锁,用于互斥WriteIndex和Sync接口.
    writelock      sync.Mutex

    // 内存索引
    mem         *MemoryIndex

    // 两个磁盘索引
    disk        []*DiskIndex

    // 两个磁盘索引的文件路径
    diskName    []string

    // 索引状态信息
    varIndexStatus     VarIndexStatus
    // 状态信息存放的文件
    statFileFullPath string

    // 上次sync操作的时间
    lastSyncTime    int64

    filePath        string
}

// 往动态库写入索引
func (this *VarIndex) WriteIndex(t TermSign,l *InvList) (error) {
    // 写操作跟同步操作互斥,只要不影响检索性能都可以忍受
    this.writelock.Lock()
    defer this.writelock.Unlock()

    return this.mem.WriteIndex(t,l)
}

// 读取索引
func (this *VarIndex) ReadIndex(t TermSign)(*InvList,error) {
    // 加多锁
    this.readLock.RLock()
    defer this.readLock.RUnlock()

    memlst,err := this.mem.ReadIndex(t)
    if err != nil {
        return nil,err
    }

    // 怎么保证this.disk[currDisk]一定可用,有可能被sync操作破坏
    // readlock保证disk[currDisk]一定可用
    disklst := NewInvListPointer()
    if this.varIndexStatus.CurrDisk >= 0 &&
        this.disk[this.varIndexStatus.CurrDisk] != nil {

        disklst,err = this.disk[this.varIndexStatus.CurrDisk].ReadIndex(t)
        if err != nil {
            return nil,err
        }
    }
    memlst.Merge(*disklst)
    return memlst,nil
}

// 同步操作.耗时加锁型操作.
func (this *VarIndex) Sync() (error) {
    // 整个同步过程暂停写操作
    this.writelock.Lock()
    defer this.writelock.Unlock()

    if this.mem.GetTermCount() <= 0 {
        // 如果内存库没索引,就不再折腾了
        return nil
    }

    now := time.Now().Unix()
    if now - this.lastSyncTime < 10 {
        // 强制限制两次sync的间隔时间
        return nil
    }

    // 目前还没有任何磁盘库
    if this.varIndexStatus.CurrDisk < 0 {
        // 假设当前磁盘库是1库
        this.varIndexStatus.CurrDisk = 1
        // 空的库
        this.disk[this.varIndexStatus.CurrDisk] = NewDiskIndex()
    }

    // 目标写入的磁盘库
    dstDisk := (this.varIndexStatus.CurrDisk + 1) % 2

    // 接下来从memory库和currDisk库读索引,合并写入dstDisk
    // 期间不影响整个动态库的ReadIndex操作
    {
        // maxFileSz 索引大文件单个文件的最大大小.
        maxFileSz := uint32(1024*1024*1024)

        // 新库预期最多的term数量
        dstTermCount := this.mem.GetTermCount() + 
            this.disk[this.varIndexStatus.CurrDisk].GetTermCount()

        if this.disk[dstDisk] != nil {
            this.disk[dstDisk].Close()
        }

        this.disk[dstDisk] = NewDiskIndex()
        err := this.disk[dstDisk].Init(this.filePath,this.diskName[dstDisk],
            maxFileSz,dstTermCount)
        if err != nil {
            return log.Error(err)
        }

        // merge (mem,currDisk) to dstDisk
        err = IndexMerge(this.mem,this.disk[this.varIndexStatus.CurrDisk],
            this.disk[dstDisk])
        if err != nil {
            return log.Error(err)
        }

        // 关闭后重新打开,后面就只读操作
        this.disk[dstDisk].Close()

        this.disk[dstDisk] = NewDiskIndex()
        err = this.disk[dstDisk].Open(this.filePath,this.diskName[dstDisk])
        if err != nil {
            return log.Error(err)
        }
    }

    // 进行两个操作
    // 期间会影响整个动态库的ReadIndex操作
    // 操作很快,采用读写锁应该问题不大吧....(我这里不是很自信这样做)
    {
        this.readLock.Lock()

        // 清空memory库
        this.mem.Clear()

        // 将当前磁盘库切换到新库,这个新库已经包含了全部动态索引
        this.varIndexStatus.CurrDisk = dstDisk

        this.readLock.Unlock()
    }
    // 现在ReadIndex已经可以正常工作了,WriteIndex也可以了

    this.lastSyncTime = time.Now().Unix()
    return this.SaveJsonFile()
    return nil
}

// 打开动态库接口.
func (this *VarIndex) Open(path string) error {

    this.filePath = path

    // 磁盘状态文件需要设置的两个步骤:(1)指示要写入的结构;(2)设置写入路径
    this.SelfStatus = &this.varIndexStatus
    this.StatusFilePath = filepath.Join(this.filePath,"var.stat")
    err := this.ParseJsonFile()
    if err != nil {
        // 解析失败
        this.varIndexStatus.CurrDisk = -1
    }

    // 磁盘索引文件
    this.diskName[0] = "var.disk0"
    this.diskName[1] = "var.disk1"

    // ----------------------------------------------------

    if this.varIndexStatus.CurrDisk >= 0 {
        // 已有磁盘索引
        // 打开当前磁盘库
        i := this.varIndexStatus.CurrDisk
        this.disk[i] = NewDiskIndex()
        err := this.disk[i].Open(this.filePath,this.diskName[i])
        if err != nil {
            return log.Error(err)
        }
        // 另外一个磁盘库暂时不需要
        j := (i + 1)%2
        this.disk[j] = nil
    } else {
        // 完全没有磁盘索引存在
        // 都标志为不可用,有需要再说
        this.varIndexStatus.CurrDisk = -1
        this.disk[0] = nil
        this.disk[1] = nil
    }

    return this.SaveJsonFile()
}

// VarIndex构造函数
func NewVarIndex() (*VarIndex) {
    s := VarIndex{}

    s.mem = NewMemoryIndex()

    s.disk = make([](*DiskIndex),2)
    s.disk[0] = nil
    s.disk[1] = nil

    s.diskName = make([]string,2)

    s.varIndexStatus.CurrDisk = -1
    s.lastSyncTime = time.Now().Unix()

    return &s
}






