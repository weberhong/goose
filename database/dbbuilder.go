package database

import (
	. "github.com/getwe/goose/utils"
    "os"
)

// 静态索引生成器.并发不安全,内部不加锁浪费性能.调用者需要保证不并发使用.
type DBBuilder struct {

    // 正排转倒排管理
    transformMgr    *IndexTransformManager

    // id管理
    idMgr           *IdManager

    // value管理
    valueMgr        *ValueManager

    // data管理
    dataMgr         *DataManager

    filePath        string
    indexFileName        string
    maxTermCnt      int
    maxId           InIdType
    valueSz         uint32
    maxDataFileSz   uint32
    maxIndexFileSz  uint32

}

// 根据唯一外部ID,分配内部ID,可并发内部有锁控制按顺序分配
func (this *DBBuilder) AllocID(outID OutIdType) (InIdType,error){
    if this.idMgr == nil {
        return 0,NewGooseError("DBBuilder","use nil handler","")
    }
    return this.idMgr.AllocID(outID)
}

// 写入索引,不可并发写入
func (this *DBBuilder) WriteIndex(InID InIdType,termlist []TermInDoc)(error){
    if this.transformMgr == nil {
        return NewGooseError("DBBuilder","use nil handler","")
    }

    return this.transformMgr.WriteIndex(InID,termlist)
}

// 写入Value数据,可并发写入
func (this *DBBuilder) WriteValue(InID InIdType,v Value) (error) {
    if this.valueMgr == nil {
        return NewGooseError("DBBuilder","use nil handler","")
    }

    return this.valueMgr.WriteValue(InID,v)
}

// 写入Data数据,可并发调用,内部锁控制
func (this *DBBuilder) WriteData(InID InIdType,d Data) (error) {
    if this.dataMgr == nil {
        return NewGooseError("DBBuilder","use nil handler","")
    }

    // dataMgr内部锁控制,并发写顺序写入
    return this.dataMgr.Append(InID,d)
}

// 进行一次数据同步.对于DBBuilder,一次同步后全部数据写入磁盘,只允许一次写入
func (this *DBBuilder) Sync() (error) {
    this.dataMgr.Close()

    this.valueMgr.Sync()

    this.idMgr.Sync()

    // 打开一个最终可写入的磁盘索引并写入全部索引
    db := NewDiskIndex()
    err := db.Init(this.filePath,this.indexFileName,this.maxIndexFileSz,
        this.transformMgr.GetTermCount())
    if err != nil {
        return err
    }
    err = this.transformMgr.Dump(db)
    if err != nil {
        return err
    }
    db.Close()

    this.transformMgr = nil
    this.idMgr = nil
    this.valueMgr = nil
    this.dataMgr = nil
    return nil
}

// 初始化工作.
// fPath:工作目录.
// MaxTermCnt:内部正排转倒排一次在内存中写入的最大term数量.
// Maxid:内部id最大上限.
// valueSz:value数据固定大小.
// maxIndexFileSz:index数据分文件每个文件的最大大小.
// maxDataFileSz:data数据分文件每个文件的最大大小.
func (this *DBBuilder) Init(fPath string,MaxTermCnt int,
    Maxid InIdType,valueSz uint32,
    maxIndexFileSz uint32,maxDataFileSz uint32) error {

    var err error

    this.filePath = fPath
    this.indexFileName = "static"
    this.maxTermCnt = MaxTermCnt
    this.maxId = Maxid
    this.valueSz = valueSz
    this.maxDataFileSz = maxDataFileSz
    this.maxIndexFileSz = maxIndexFileSz

    if _,err := os.Stat(this.filePath); os.IsNotExist(err) {
        err := os.MkdirAll(this.filePath,0755)
        if err != nil {
            return err
        }
    }


    err = this.transformMgr.Init(fPath,MaxTermCnt)
    if err != nil { return err }

    err = this.idMgr.Init(fPath,Maxid)
    if err != nil { return err }

    err = this.valueMgr.Init(fPath,Maxid,valueSz)
    if err != nil { return err }

    err = this.dataMgr.Init(fPath,Maxid,maxDataFileSz)
    if err != nil { return err }

    return nil
}


func NewDBBuilder() (*DBBuilder) {
    db := DBBuilder{}

    db.transformMgr = NewIndexTransformManager()
    db.idMgr        = NewIdManager()
    db.valueMgr     = NewValueManager()
    db.dataMgr      = NewDataManager()

    return &db
}




