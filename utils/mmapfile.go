package utils

import (
    "os"
    "encoding/binary"
	"github.com/edsrzf/mmap-go"
    "path/filepath"
    "reflect"
    "fmt"
)

type MmapFile struct {
    // 文件路径
    filePath    string
    // 文件名
    fileName    string
    // 文件指针
    fileHandle  *os.File
    // mmap指针
    fileMmap    mmap.MMap
    // 文件大小(单位byte),uint32表示文件大小不能超过4G
    fileSize    uint32
}

// mmap方式打开文件,文件最大大小为fsize(单位byte)
// 如果文件已经存在,则直接打开
// 如果文件不存在,则新建
func (this *MmapFile) OpenFile(path string,name string,fsize uint32) (error) {
    // 打开文件
    f,err := os.OpenFile(filepath.Join(path,name),os.O_RDWR|os.O_CREATE,0644)
    if err != nil {
        return NewGooseError("MmapFile.OpenFile","os.OpenFile",err.Error())
    }

    fstat,_ := f.Stat()
    if fstat.Size() < int64(fsize) {
        // 产生空洞
        _,err = f.Seek(int64(fsize-1),0)
        if err != nil {
            return NewGooseError("MmapFile.OpenFile","file seek",err.Error())
        }
        _,err = f.WriteString(" ")
        if err != nil {
            return NewGooseError("MmapFile.OpenFile","WriteString",err.Error())
        }
    }
    // FIXME 文件实际大小比要映射的区间还大,应该?
    // 不进行截断,全部mmap纯浪费内存了
    // fstat.Size() > int64(fsize)

    // 映射
    this.fileMmap,err = mmap.Map(f,mmap.RDWR,0)
    if err != nil {
        return NewGooseError("MmapFile.OpenFile","mmap.Map",err.Error())
    }

    // 映射后,相当于分配了一个[]byte,其大小应该是等于fsize
    if uint64(len(this.fileMmap)) != uint64(fsize) {
        return NewGooseError("Mmapfile","fileMmap length error",
            fmt.Sprintf("[%d] != [%d]",len(this.fileMmap),fsize))
    }

    this.filePath = path
    this.fileName = name
    this.fileSize = fsize
    this.fileHandle = f

    return nil
}

func (this *MmapFile) Close() (error) {
    err := this.fileMmap.Unmap()
    if err != nil {
        return err
    }
    this.fileHandle.Close()
    return nil
}

func (this *MmapFile) Flush() (error) {
    err := this.fileMmap.Flush()
    if err != nil {
        return err
    }
    return nil
}

// 从offset位置开始写入数字n,占用空间大小根据n反射类型决定
func (this *MmapFile) WriteNum(offset uint32,n interface{}) (error) {
    destType,destSz := IntKindSize(n)
    if destSz == 0 {
        return NewGooseError("Mmapfile.WriteNum","not a num","")
    }
    if int64(offset) + int64(destSz) > int64(len(this.fileMmap)) {
        return NewGooseError("Mmapfile.WriteNum","over length limit","")
    }

    // 从map中分配一个[]byte,已经保证长度足够
    // 后面写操作不会引发内存拷贝
    buf := this.fileMmap[offset:offset+uint32(destSz)]

    order := binary.BigEndian
    num := reflect.ValueOf(n)
    if !num.IsValid() {
        return NewGooseError("Mmapfile.WriteNum","Invalid num","")
    }
    switch destType {
    case reflect.Int8:
        buf[0] = byte(num.Int())
    case reflect.Uint8:
        buf[0] = byte(num.Uint())
    case reflect.Int16:
        order.PutUint16(buf, uint16(num.Int()))
    case reflect.Uint16:
        order.PutUint16(buf, uint16(num.Uint()))
    case reflect.Int32:
        order.PutUint32(buf, uint32(num.Int()))
    case reflect.Uint32:
        order.PutUint32(buf, uint32(num.Uint()))
    case reflect.Int64:
        order.PutUint64(buf, uint64(num.Int()))
    case reflect.Uint64:
        order.PutUint64(buf, uint64(num.Uint()))
    default:
        return NewGooseError("MmapFile.WriteNum","Wrong Type","")
    }
  
    /*
    switch destSz {
    case 1:
        buf[0] = byte(num.Uint())
    case 2:
        order.PutUint16(buf,uint16(num.Uint()))
    case 4:
        order.PutUint32(buf,uint32(num.Uint()))
    case 8:
        order.PutUint64(buf,uint64(num.Uint()))
    default:
        return NewGooseError("MmapFile.WriteNum","Wrong Type","")
    }
    */
    return nil
     /*
    switch v := n.(type) {
    case int8:
        buf[0] = byte(v)
    case uint8:
        buf[0] = byte(v)
    case int16:
        order.PutUint16(buf, uint16(v))
    case uint16:
        order.PutUint16(buf, v)
    case int32:
        order.PutUint32(buf, uint32(v))
    case uint32:
        order.PutUint32(buf, v)
    case int64:
        order.PutUint64(buf, uint64(v))
    case uint64:
        order.PutUint64(buf, v)
    default:
        return NewGooseError("MmapFile.WriteNum","Wrong Type","")
    }
    return nil
    */
}

// 对ReadNum的封装,方便外部直接使用
func (this *MmapFile) ReadUint8(offset uint32) (uint8,error) {
    n,err := this.ReadNum(offset,1)
    return uint8(n),err
}

func (this *MmapFile) ReadUint32(offset uint32) (uint32,error) {
    n,err := this.ReadNum(offset,4)
    return uint32(n),err
}

func (this *MmapFile) ReadUint64(offset uint32) (uint64,error) {
    n,err := this.ReadNum(offset,8)
    return uint64(n),err
}


// 读取offset开始的destSz个字节作为数字返回
func (this *MmapFile) ReadNum(offset uint32,destSz uint32) (uint64,error) {
    if destSz == 0 {
        return 0,NewGooseError("Mmapfile.ReadNum","not a basic num","")
    }
    if int64(offset) + int64(destSz) > int64(len(this.fileMmap)) {
        return 0,NewGooseError("Mmapfile.ReadNum","over length limit","")
    }

    buf := this.fileMmap[offset:offset+uint32(destSz)]

    order := binary.BigEndian
    switch destSz {
    case 1:
        return uint64(buf[0]),nil
    case 2:
        return uint64(order.Uint16(buf)),nil
    case 4:
        return uint64(order.Uint32(buf)),nil
    case 8:
        return uint64(order.Uint64(buf)),nil
    default:
        return 0,NewGooseError("MmapFile.ReadNum","Wrong Type","")
    }
    return 0,nil
    /*
    switch v := n.(type) {
    case *int8:
        *v = int8(buf[0])
    case *uint8:
        *v = buf[0]
    case *int16:
        *v = int16(order.Uint16(buf))
    case *uint16:
        *v = uint16(order.Uint16(buf))
    case *int32:
        *v = int32(order.Uint32(buf))
    case *uint32:
        *v = uint32(order.Uint32(buf))
    case *int64:
        *v = int64(order.Uint64(buf))
    case *uint64:
        *v = uint64(order.Uint64(buf))
    default:
        return NewGooseError("MmapFile.ReadNum","Wrong Type","")
    }

    return nil
    */
}

// write max length bytes
func (this *MmapFile) WriteBytes(offset uint32,buf []byte,length uint32)(error){

    if length > uint32(len(buf)) {
        length = uint32(len(buf))
    }

    if uint64(offset + length) > uint64(len(this.fileMmap)) {
        return NewGooseError("Mmapfile.WriteBytes","over length limit","")
    }

    var i uint32
    for i=0;i<length;i++ {
        this.fileMmap[offset + i] = buf[i]
    }
    return nil
}

// read bytes (reference) 
func (this *MmapFile) ReadBytes(offset uint32,length uint32) ([]byte,error) {
    if uint64(offset + length) > uint64(len(this.fileMmap)) {
        return nil,NewGooseError("Mmapfile.ReadBytes","over length limit","")
    }

    return this.fileMmap[offset:offset+length],nil
}

// read bytes (copy slice)
func (this *MmapFile) ReadBytesCopy(offset int32,length int32) ([]byte,error) {
    if int64(offset + length) > int64(len(this.fileMmap)) {
        return nil,NewGooseError("Mmapfile.ReadBytes","over length limit","")
    }

    newbuf := make([]byte,length)
    copy(newbuf,this.fileMmap[offset:offset+length])
    return newbuf,nil
}


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
