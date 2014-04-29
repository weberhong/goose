package utils

import (
	"fmt"
    "path/filepath"
    "os"
    "io"
    "encoding/binary"
    log "github.com/getwe/goose/log"
)

const (
    statFileSuffix = ".bigfile.stat"
    dataFileSuffix = ".bigfile.n"
    bigFileModelInit = iota
    bigFileModelOpen
)

// 大文件索引
type BigFileIndex struct {
    FileNo  uint8    /// 文件编号
    Offset  uint32   /// 数据存储地址偏移量
    Length  uint32   /// 数据长度
}

func (this *BigFileIndex) Decode(buf []byte) (error) {
    order := binary.BigEndian
    if len(buf) < 9 {
        return log.Error("BigFileIndex.Decode buf length [%d] error",len(buf))
    }

    this.FileNo = uint8(buf[0])
    this.Offset = uint32(order.Uint32(buf[1:5]))
    this.Length = uint32(order.Uint32(buf[5:9]))
    return nil
}

func (this *BigFileIndex) Encode(buf []byte) (error) {
    order := binary.BigEndian
    if len(buf) < 9 {
        return log.Error("BigFileIndex.Decode buf length [%d] error",len(buf))
    }

    buf[0] = this.FileNo
    order.PutUint32(buf[1:5],this.Offset)
    order.PutUint32(buf[5:9],this.Length)
    return nil
}

// 记录大逻辑文件的一些必须信息
type BigFileStat struct {
    FileCnt         uint8    /// 由多少个物理文件组成
    LastFileOffset  uint32   /// 最后一个文件的文件偏移量
    SuggestFileSize uint32   /// 建议的一个物理文件的最大大小
}
func (this *BigFileStat) Reset() {
    this.FileCnt = 0
    this.LastFileOffset = 0
    this.SuggestFileSize = 0
}

// 由多个小文件组成的逻辑大文件
// 支持追加写操作 : 只能把数据追加写到逻辑大文件的末尾,也就是最后一个文件末尾
// 支持读操作 : 可以读取逻辑大文件的任何一部分,但是不支持跨物理子文件
// 就是为了goose的data二级索引而生,没啥通用性,就是封装而已
type BigFile struct {
    // 磁盘存储目录
    filePath        string
    // 文件名前缀
    fileName        string

    // 完成写操作后只允许读操作的文件指针数组
    readOnlyFile    []*os.File

    // 允许读写操作的文件
    readwriteFile   *os.File

    // 逻辑大文件的状态
    bigfileStat     BigFileStat

    // 状态标识文件全路径
    statFileFullPath        string

    // 当前支持读写的最后一个文件的全路径
    readwriteFileFullPath   string

    // 逻辑状态,两种状态
    // 1.全新初始化状态
    // 2.打开以有数据状态
    fileModel       int
}

// 打开已存在的大文件,如果不存在,直接返回错误
func (this *BigFile) Open(path string,name string) (error) {

    // 是打开已有数据文件状态
    this.fileModel = bigFileModelOpen

    this.filePath = path
    this.fileName = name

    this.statFileFullPath = filepath.Join(this.filePath,
        fmt.Sprintf("%s%s",this.fileName,statFileSuffix))

    // 解析获取文件信息
    err := this.parseStatFile()
    if err != nil {
        return log.Warn(err)
    }
    // 检验状态文件
    if this.bigfileStat.SuggestFileSize == 0 ||
       this.bigfileStat.FileCnt == 0 ||
       this.bigfileStat.LastFileOffset == 0 {
        return log.Error("BigFile.Open stat file error")
    }

    // 除了最后一个文件,其它以只读方式打开
    readOnlyFileCnt := this.bigfileStat.FileCnt - 1
    this.readOnlyFile = make([]*os.File,readOnlyFileCnt)
    for i:=0; uint8(i)<readOnlyFileCnt;i++ {
        f,err := this.openRoFile(uint8(i))
        if err != nil {
            return err
        }
        this.readOnlyFile[i] = f
        // 校验这些只读文件的大小,他们肯定是大于等于配置才对
        // TODO
    }

    // 最后一个文件已读写方式打开
    err = this.openRwFile(this.bigfileStat.FileCnt - 1)
    if err != nil {
        return err
    }
    // 设置文件指针
    this.readwriteFile.Seek(int64(this.bigfileStat.LastFileOffset),0)

    // 最后一个文件的文件指针应该就是文件大小
    sz,_ := FileSize(this.readwriteFile)
    if sz != int64(this.bigfileStat.LastFileOffset) {
        return log.Error("BigFile.Open","FileStatInfo Error LastFileOffset:[%d] != FileSize:[%d]",
            this.bigfileStat.LastFileOffset,sz)
    }

    return nil
}

// 初始化,抛弃已有的任何数据
// maxFileSz : 建议物理文件的大小
func (this *BigFile) Init(path string,name string,maxFileSz uint32) (error) {

    // 全新初始化
    this.fileModel = bigFileModelInit

    this.filePath = path
    this.fileName = name

    this.statFileFullPath = filepath.Join(this.filePath,
    fmt.Sprintf("%s%s",this.fileName,statFileSuffix))

    // 清空
    this.bigfileStat.Reset()
    this.bigfileStat.SuggestFileSize = maxFileSz

    // 全新数据文件初始化
    this.readOnlyFile = make([]*os.File,0)
    this.readwriteFile = nil

    // 将状态文件写入磁盘
    err := this.saveStatFile()
    if err != nil {
        return err
    }

    return nil
}

// 获取逻辑文件信息
func (this *BigFile) GetStatInfo() (BigFileStat) {
    this.parseStatFile()
    return this.bigfileStat
}

// 追加数据,返回追加数据的存储信息.不可并发进行写操作.
func (this *BigFile) Append(buf []byte)(*BigFileIndex,error) {

    f,err := this.getRwFile()
    if err != nil {
        return nil,err
    }

    i := BigFileIndex{}
    i.FileNo = this.bigfileStat.FileCnt - 1
    i.Length = uint32(len(buf))
    off,err := this.readwriteFile.Seek(0,1)
    i.Offset = uint32(off)
    if i.Offset != this.bigfileStat.LastFileOffset {
        return nil,log.Error("BigFile.Append getOffset[%d] LastFileOffset[%d]",
                i.Offset,this.bigfileStat.LastFileOffset)
    }

    n,err := f.Write(buf)
    if err != nil {
        return nil,log.Error("BigFile.Append write fail : %s",err.Error())
    }
    if uint32(n) != i.Length {
        // 写成功,但是写入长度跟期望对不上
        // 回滚文件指针
        this.readwriteFile.Seek(int64(i.Offset),0)
        return nil,log.Error("BigFile.Append write succ bug length error : %s",
            err.Error())
    }
    // 更新状态文件
    this.bigfileStat.LastFileOffset = i.Offset + i.Length
    this.saveStatFile()

    return &i,nil
}

// 读取数据,外部需要准备好够存放的desBuf
func (this *BigFile) Read(i BigFileIndex,desBuf []byte)(error) {
    if i.FileNo >= this.bigfileStat.FileCnt {
        return log.Error("BigFile.Read FileNo[%d] Error",i.FileNo)
    }
    if i.Length > uint32(len(desBuf)) {
        return log.Error("BigFile.Read BigFileIndex.Length[%d] > len(desBuf)[%d]",
            i.Length,uint32(len(desBuf)))
    }

    var f *os.File
    if i.FileNo == this.bigfileStat.FileCnt - 1 {
        f = this.readwriteFile
    } else {
        f = this.readOnlyFile[i.FileNo]
    }
    n,err := f.ReadAt(desBuf[:i.Length],int64(i.Offset))
    if err == io.EOF {
        if uint32(n) == i.Length {
            // 刚刚好读完
            return nil
        }
    }
    if uint32(n) != i.Length {
        return log.Error("Read Length Error offset[%d] destBuf len[%d],ReadAt len[%d]",
            i.Offset,i.Length,n)
    }
    if err != nil {
        return log.Error("ReadAt file",err.Error())
    }
    return nil
}

// 关闭文件
func (this *BigFile) Close() {

    for _,v := range this.readOnlyFile {
        if v != nil {
            v.Close()
        }
    }
    if this.readwriteFile != nil {
        this.readwriteFile.Close()
    }
}

//  
func (this *BigFile) Sync() (error) {

    for _,v := range this.readOnlyFile {
        if v != nil {
            v.Sync()
        }
    }
    if this.readwriteFile != nil {
        this.readwriteFile.Sync()
    }
    return nil
}

func (this *BigFile) parseStatFile() (error) {
    this.bigfileStat.FileCnt = 0
    this.bigfileStat.LastFileOffset = 0
    this.bigfileStat.SuggestFileSize = 0
    return JsonDecodeFromFile(&this.bigfileStat,this.statFileFullPath)
}

func (this *BigFile) saveStatFile() (error) {
    return JsonEncodeToFile(this.bigfileStat,this.statFileFullPath)
}

func (this *BigFile) getRwFile() (*os.File,error) {
    // 没有可读写的文件,创建新的
    if this.readwriteFile == nil {
        err := this.addRwFile()
        if err != nil {
            return nil,err
        }
        return this.readwriteFile,nil
    }

    rwFileSz,err := FileSize(this.readwriteFile)
    if err != nil {
        return nil,err
    }
    // 读写文件太大了,该分配新文件了
    if uint64(rwFileSz) >= uint64(this.bigfileStat.SuggestFileSize) {
        err := this.addRwFile()
        if err != nil {
            return nil,err
        }
        return this.readwriteFile,nil
    }
    return this.readwriteFile,nil
}

func (this *BigFile) addRwFile() (error) {
    // 原来已经有打开过读写文件,改成只打开方式
    if this.readwriteFile != nil {
        this.readwriteFile.Sync()
        this.readwriteFile.Close()

        f,err := this.openRoFile(this.bigfileStat.FileCnt - 1)
        if err != nil {
            return err
        }
        this.readOnlyFile = append(this.readOnlyFile,f)
        // 当前文件完成使命
        this.readwriteFile = nil

        // 等这个函数都走完了才可以增加文件数
        // this.bigfileStat.FileCnt++

    }

    // 开始打开新的rw文件
    // assert (this.readwriteFile == nil)
    err := this.openRwFile(this.bigfileStat.FileCnt)
    if err != nil {
        this.readwriteFile = nil
        // 这里失败了,相当于已有的文件都是只读打开,读写文件为nil
        // 逻辑可以继续正常跑下去
        return err
    }

    // 分配新的rw文件正式成功了!
    this.bigfileStat.FileCnt++
    // 新文件offset肯定是0,其它地方还会再做校验工作
    this.bigfileStat.LastFileOffset = 0
    return nil
}

// 根据文件数量打开最后一个可读写文件
func (this *BigFile) openRwFile(fileno uint8) (error) {
    var err error
    tname := fmt.Sprintf("%s%s%d",this.fileName,dataFileSuffix,fileno)
    this.readwriteFileFullPath = filepath.Join(this.filePath,tname)
    if this.fileModel == bigFileModelInit {
        // 全新初始化的,文件打开后直接做截断处理
        this.readwriteFile,err = os.OpenFile(this.readwriteFileFullPath,
            os.O_RDWR|os.O_CREATE|os.O_TRUNC,0644)
    } else if this.fileModel == bigFileModelOpen {
        // 已有文件,追加写
        this.readwriteFile,err = os.OpenFile(this.readwriteFileFullPath,
            os.O_RDWR|os.O_CREATE|os.O_APPEND,0644)
    } else {
        return log.Error("File Status Error : [%d]",this.fileModel)
    }
    if err != nil {
        return log.Error("open readwrite file fail : %s",err.Error())
    }
    return nil
}

func (this *BigFile) openRoFile(fileno uint8) (*os.File,error) {
    var err error
    tname := fmt.Sprintf("%s%s%d",this.fileName,dataFileSuffix,fileno)
    f,err := os.OpenFile(filepath.Join(this.filePath,tname),os.O_RDONLY,0644)
    if err != nil {
        return nil,log.Error("open readonly file fail : %s",err.Error())
    }
    return f,nil
}




/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
