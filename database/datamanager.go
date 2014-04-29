package database

import (
    . "github.com/getwe/goose/utils"
    "encoding/binary"
    "path/filepath"
    "fmt"
    log "github.com/getwe/goose/log"
)

// data磁盘数据文件自描述所需的字段
type DataManagerStatus struct {
    // 最大id
    MaxInId             InIdType
 }



// data数据管理,每个结果的data可以是变长的,数据结构需要设计为二级索引.
// 并发安全性问题:读操作可并发,写操作不可并发.
// 全部data数据合起来会非常大,需要多文件存储
// 一级索引(定长): [fileno,offset,length][fileno,offset,length] ... [fileno,offset,length]
// 二级索引(变长): [data][data][data] ... [data] 分隔存储到多个文件
type DataManager struct {
    JsonStatusFile

    // 磁盘存储目录
    filePath        string

    // 数据文件最大大小
    maxDataFileSize     uint32

    // 一级索引
    data0               MmapFile

    // 二级索引
    data1               *BigFile

    // 本身status
    dataStatus          DataManagerStatus
}


// 打开已存在的数据文件
func (this *DataManager) Open(path string) (error) {

    this.filePath = path

    // 磁盘状态文件需要设置的两个步骤:(1)指示要写入的结构;(2)设置写入路径
    this.SelfStatus = &this.dataStatus
    this.StatusFilePath = filepath.Join(this.filePath,"data.stat")
    err := this.ParseJsonFile()
    if err != nil {
        return err
    }


    // 一级索引mmap打开
    data0Size := uint32(this.dataStatus.MaxInId) * uint32(binary.Size(BigFileIndex{}))
    data0Name := fmt.Sprintf("data.d0")
    err = this.data0.OpenFile(this.filePath,data0Name,data0Size)
    if err != nil {
        return log.Error("mmap open[%s] size[%d] fail : %s",data0Name,data0Size,err)
    }
    // 二级索引BigFile打开
    this.data1 = new(BigFile)
    data1Name := fmt.Sprintf("data.d1")
    err = this.data1.Open(this.filePath,data1Name)
    if err != nil {
        return err
    }

    return nil
}

// 全新初始化数据文件
func (this *DataManager) Init(path string,maxId InIdType,maxFileSz uint32) (error) {

    this.dataStatus.MaxInId = maxId
    this.maxDataFileSize = maxFileSz
    this.filePath = path

    // 磁盘状态文件需要设置的两个步骤:(1)指示要写入的结构;(2)设置写入路径
    this.SelfStatus = &this.dataStatus
    this.StatusFilePath = filepath.Join(this.filePath,"data.stat")


    // 一级索引mmap打开
    data0Size := uint32(this.dataStatus.MaxInId) * uint32(binary.Size(BigFileIndex{}))
    data0Name := fmt.Sprintf("data.d0")
    err := this.data0.OpenFile(this.filePath,data0Name,data0Size)
    if err != nil {
        return log.Error("mmap open[%s] size[%d] fail : %s",data0Name,data0Size,err)
    }
    // 二级索引BigFile打开
    this.data1 = new(BigFile)
    data1Name := fmt.Sprintf("data.d1")
    err = this.data1.Init(this.filePath,data1Name,this.maxDataFileSize)
    if err != nil {
        return err
    }

    return this.SaveJsonFile()
}


func (this *DataManager) Sync() (error) {

    err := this.data0.Flush()
    if err != nil {
        return err
    }

    err = this.data1.Sync()
    if err != nil {
        return err
    }
    return err
}

func (this *DataManager) Close() (error) {
    this.data0.Close()
    this.data1.Close()
    return nil
}

// 追加数据文件.不可并发写入,使用者应该自己做好并发控制.
// 同一个InId多次写入会进行覆盖操作,只有最后一次写操作数据有效,而且之前的写入的
// 数据会变成垃圾数据占用磁盘空间,无法删除.
func (this *DataManager) Append(inId InIdType,d Data) (error) {
    if inId > this.dataStatus.MaxInId{
        return log.Error("inId [%d] illegal MaxInId[%d]",inId,this.dataStatus.MaxInId)
    }

    // 写二级索引
    d1Index,err := this.data1.Append(d)
    if err != nil {
        return err
    }
    if d1Index.Length != uint32(len(d)) {
        return log.Error("Write data1 datalen[%d],writelen[%d]",len(d),d1Index.Length)
    }

    // 写一级索引
    err = this.writeData0(*d1Index,inId)
    if err != nil {
        return err
    }

    return nil
}

// 读取Data数据,可以并发.
func (this *DataManager) ReadData(inId InIdType,buf *Data) (error) {
    if inId == 0 || inId > this.dataStatus.MaxInId{
        return log.Error("inId [%d] illegal MaxInId[%d]",inId,this.dataStatus.MaxInId)
    }

    // 读一级索引
    bigFileI,err := this.readData0(inId)
    if err != nil {
        return err
    }
    if bigFileI.Length == 0 {
        return log.Error("Read data0 inId[%d],fileNo[%d],length[%d],offset[%d]",
            inId,bigFileI.FileNo,bigFileI.Length,bigFileI.Offset)
    }


    // 读二级索引
    if bigFileI.Length > uint32(buf.Len()) {
        *buf = NewData(int(bigFileI.Length))
    }
    err = this.data1.Read(bigFileI,*buf)
    if err != nil {
        return err
    }

    return nil
}


func (this *DataManager) writeData0(d0 BigFileIndex,inId InIdType) (error) {
    pos := uint32(inId) * uint32(binary.Size(BigFileIndex{}))
    err := this.data0.WriteNum(pos,d0.FileNo)
    if err != nil {
        return err
    }
    pos += uint32(binary.Size(d0.FileNo))
    err = this.data0.WriteNum(pos,d0.Offset)
    if err != nil {
        return err
    }
    pos += uint32(binary.Size(d0.Offset))
    err = this.data0.WriteNum(pos ,d0.Length)
    if err != nil {
        return err
    }
    return nil
}

func (this *DataManager) readData0(inId InIdType) (BigFileIndex,error) {
    var d0 BigFileIndex
    var err error
    filenoSize := uint32(binary.Size(d0.FileNo))
    offsetSize := uint32(binary.Size(d0.Offset))
    lengthSize := uint32(binary.Size(d0.Length))


    // read fileno
    pos := uint32(inId) * uint32(binary.Size(BigFileIndex{}))
    fileno,err := this.data0.ReadNum(pos,filenoSize)
    if err != nil {
        return d0,err
    }

    // read fileoffset
    pos += filenoSize
    offset,err := this.data0.ReadNum(pos,offsetSize)
    if err != nil {
        return d0,err
    }

    // read filelength
    pos += offsetSize
    length,err := this.data0.ReadNum(pos,lengthSize)
    if err != nil {
        return d0,err
    }


    d0.FileNo = uint8(fileno)
    d0.Offset = uint32(offset)
    d0.Length = uint32(length)

    return d0,nil
}

func NewDataManager() (*DataManager) {
    data := DataManager{}

    return &data
}


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
