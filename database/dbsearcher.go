package database

import (
	. "github.com/getwe/goose/utils"
    log "github.com/getwe/goose/log"
)

type DBSearcher struct {

    // 静态索引库
    staticIndex     *StaticIndex

    // 动态索引库
    varIndex        *VarIndex

    // id管理
    idMgr           *IdManager

    // value管理
    valueMgr        *ValueManager

    // data管理
    dataMgr         *DataManager

    // 工作目录
    filePath        string
}

// 根据唯一外部ID,分配内部ID,可并发内部有锁控制按顺序分配
func (this *DBSearcher) AllocID(outID OutIdType) (InIdType,error){
    if this.varIndex == nil {
        return 0,log.Error("No Var Index")
    }
    if this.idMgr == nil {
        return 0,log.Error("no id manager")
    }
    return this.idMgr.AllocID(outID)
}

func (this *DBSearcher) GetOutID(inId InIdType)(OutIdType,error) {
    if this.idMgr == nil {
        return 0,log.Error("no id manager")
    }
    return this.idMgr.GetOutID(inId)
}

// 写入索引,不可并发写入.
func (this *DBSearcher) WriteIndex(InID InIdType,termlist []TermInDoc)(error){
    if this.varIndex == nil {
        return log.Error("No Var Index")
    }
    for _,term := range termlist {
        l := NewInvList(1)
        l.Append(Index{ InID : InID,Weight : term.Weight})
        this.varIndex.WriteIndex(term.Sign,&l)
    }
    return nil
}

// 读取索引,可并发
func (this *DBSearcher) ReadIndex(t TermSign)(*InvList,error) {
    var err error
    var staticlist *InvList
    var varlist *InvList
    if this.staticIndex != nil {
        staticlist,err = this.staticIndex.ReadIndex(t)
        if err != nil {
            staticlist = NewInvListPointer(0)
        }
    }

    if this.varIndex != nil {
        varlist,err = this.varIndex.ReadIndex(t)
        if err != nil {
            varlist = NewInvListPointer(0)
        }
    }

    staticlist.Merge(*varlist)
    return staticlist,nil
}


// 写入Value数据,可并发写入.
func (this *DBSearcher) WriteValue(InID InIdType,v Value) (error) {
    if this.varIndex == nil {
        return log.Error("No Var Index")
    }
     if this.valueMgr == nil {
        return log.Error("no value manager")
    }

    return this.valueMgr.WriteValue(InID,v)
}

// 读取Value数据,可并发读.value只能进行读操作,任何写操作都是非法的
func (this *DBSearcher) ReadValue(inId InIdType)(Value,error) {
    if this.valueMgr == nil {
        return nil,log.Error("no value manager")
    }
    return this.valueMgr.ReadValue(inId)
}

// 写入Data数据,可并发调用.
func (this *DBSearcher) WriteData(InID InIdType,d Data) (error) {
    if this.varIndex == nil {
        return log.Error("No Var Index")
    }
     if this.dataMgr == nil {
        return log.Error("no data manager")
    }

    // dataMgr内部锁控制,并发写顺序写入
    return this.dataMgr.Append(InID,d)
}

// 读取Data数据,可并发调用.
func (this *DBSearcher) ReadData(inId InIdType,buf *Data) (error) {
    if this.dataMgr == nil {
        return log.Error("no data manager")
    }
    return this.dataMgr.ReadData(inId,buf)
}

// 初始化工作.
// fPath:工作目录.
func (this *DBSearcher) Init(fPath string) (error) {
    var err error
    this.filePath = fPath

    // data
    err = this.dataMgr.Open(this.filePath)
    if err != nil { return err }

    // value
    err = this.valueMgr.Open(this.filePath)
    if err != nil { return err }

    // id
    err = this.idMgr.Open(fPath)
    if err != nil { return err }

    // static index
    err = this.staticIndex.Open(this.filePath)
    if err != nil { return err }

    // var index
    err = this.varIndex.Open(this.filePath)
    if err != nil { return err }

    return nil
}

// 进行一次数据同步.在支持动态库情况下进行一次磁盘同步
func (this *DBSearcher) Sync() (error) {
    // for var index
    err := this.varIndex.Sync()
    if err != nil {
        return err
    }
    return nil
}

func NewDBSearcher() (*DBSearcher) {
    db := DBSearcher{}

    db.dataMgr = NewDataManager()
    db.valueMgr = NewValueManager()
    db.idMgr = NewIdManager()
    db.staticIndex = NewStaticIndex()
    db.varIndex = NewVarIndex()

    return &db
}
