package goose

import (
    . "github.com/getwe/goose/utils"
    . "github.com/getwe/goose/database"
    "sync"
    "runtime"
    log "github.com/getwe/goose/log"
)

type DocIterator interface {
    // 获取下个待索引的doc,返回nil表示迭代结束.
    NextDoc() interface{}
}

// 原始数据经过策略ParseDoc的产物
type docParsed struct {
    outId OutIdType
    termList []TermInDoc
    value *Value
    data *Data
}

// 静态索引生成类.
type StaticIndexer struct {

    lock sync.Mutex

    // write only dababase
    db DataBaseWriter

    // 建索引的策略逻辑
    strategy IndexStrategy

    // 利用chan控制IndexStrategy.ParseDoc的并发数量
    parseDocRoutineNum  int
    // 控制用的chan
    parseDocChan        chan interface{}

    // ParseDoc后待写入db的队列长度
    writeDbQueueNum     int
    writeDbQueue        chan (*docParsed)

    // 等待全部doc完成解析并写入db的事件
    finishedWg          sync.WaitGroup
}

// 并发多个协程分析doc,最终阻塞完成写入db后返回
func (this *StaticIndexer) BuildIndex(iter DocIterator) (error) {
    // 整个建库过程中加锁
    this.lock.Lock()
    defer this.lock.Unlock()
    // 工作流程:
    // GetDoc --parseDocChan--> N*parseOneDoc --writeDbQueue--> 1*writeDoc

    // 初始化
    // 完成处理的doc计数
    this.finishedWg = sync.WaitGroup{}
    this.parseDocChan = make(chan interface{},this.parseDocRoutineNum)
    this.writeDbQueue = make(chan (*docParsed),this.writeDbQueueNum)

    // 启动一个写入db协程
    go this.writeDoc()

    // 启动N个解析doc协程
    for i:=0;i<this.parseDocRoutineNum;i++ {
        go this.parseDoc()
    }

    // 把全部待处理的doc都塞入parseDocChan处理
    oneDoc := iter.NextDoc()
    for ;oneDoc != nil;oneDoc = iter.NextDoc() {
        this.finishedWg.Add(1)
        this.parseDocChan <- oneDoc
    }

    // 等待全部doc处理完成的事件.
    // doc处理完有两种情况:(1)成功写入db;(2)出现错误,略过该doc.
    // 无论那种情况都应该调用一次finishedWg.Done(),最终才能触发Wait事件.
    this.finishedWg.Wait()

    // 关闭chan,使得writeOneDoc,parseOneDoc协程退出
    close(this.parseDocChan)
    close(this.writeDbQueue)
    return nil
}

func (this *StaticIndexer) parseDoc(){

    // context
    context := NewStyContext()

    // 一直从chan中获取doc,直到这个chan被close
    for doc := range this.parseDocChan {
        var err error
        // parse
        parseRes := &docParsed{}
        parseRes.outId,parseRes.termList,parseRes.value,parseRes.data,
        err = this.strategy.ParseDoc(doc,context)
        if err != nil {
            log.Error(err)
            parseRes = nil
        }
        // 打印策略日志
        context.log.PrintAllInfo()

        // toWriteDbQueue是待写入db的队列.
        // 阻塞等待队列有空余位置然后写入队列.
        this.writeDbQueue <- parseRes
    }
    log.Info("Finish parseDoc , goroutine exit.")
}

func (this *StaticIndexer) writeDoc() {

    for parseRes := range this.writeDbQueue {

        if parseRes == nil {
            log.Error("get nil pointer from queue")
            this.finishedWg.Done()
            continue
        }

        // id
        inId,err := this.db.AllocID(parseRes.outId)
        if err != nil {
            log.Error(err)
            this.finishedWg.Done()
            continue
        }

        // index 
        err = this.db.WriteIndex(inId,parseRes.termList)
        if err != nil {
            log.Error(err)
            this.finishedWg.Done()
            continue
        }

        // value
        err = this.db.WriteValue(inId,*(parseRes.value))
        if err != nil {
            log.Error(err)
            this.finishedWg.Done()
            continue
        }

        // data
        err = this.db.WriteData(inId,*(parseRes.data))
        if err != nil {
            log.Error(err)
            this.finishedWg.Done()
            continue
        }
        this.finishedWg.Done()
    }
    log.Info("Finish writeDoc,goroutine exit.")
}

// 
func NewStaticIndexer(db DataBaseWriter,sty IndexStrategy) (*StaticIndexer,error) {
    i := StaticIndexer{}
    i.db = db
    i.strategy = sty

    // 根据cpu数量决定并发分析doc的协程数量
    i.parseDocRoutineNum = runtime.NumCPU()

    // 排队写入db的队列长度
    i.writeDbQueueNum = i.parseDocRoutineNum * 2


    return &i,nil
}

// 动态索引生成类.
type VarIndexer struct {

    lock sync.Mutex

    // write only dababase
    db DataBaseWriter

    // 建索引的策略逻辑
    strategy IndexStrategy

}

// 单协程完成工作,分析doc然后写入索引结束.
func (this *VarIndexer) BuildIndex(iter DocIterator) (error) {
    // 整个建库过程中加锁
    this.lock.Lock()
    defer this.lock.Unlock()

    context := NewStyContext()

    oneDoc := iter.NextDoc()
    for ;oneDoc != nil;oneDoc = iter.NextDoc() {

        context.Clear()

        var err error
        // parse
        parseRes := &docParsed{}
        parseRes.outId,parseRes.termList,parseRes.value,parseRes.data,
        err = this.strategy.ParseDoc(oneDoc,context)
        if err != nil {
            return err
        }

        // 打一行策略的所有日志
        context.log.PrintAllInfo()

        // id
        inId,err := this.db.AllocID(parseRes.outId)
        if err != nil {
            return err
        }

        // index 
        err = this.db.WriteIndex(inId,parseRes.termList)
        if err != nil {
            return err
        }

        // value
        err = this.db.WriteValue(inId,*(parseRes.value))
        if err != nil {
            return err
        }

        // data
        err = this.db.WriteData(inId,*(parseRes.data))
        if err != nil {
            return err
        }

    }

    return nil
}

func NewVarIndexer(db DataBaseWriter,sty IndexStrategy) (*VarIndexer,error) {
    i := VarIndexer{}
    i.db = db
    i.strategy = sty
    return &i,nil
}







/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
