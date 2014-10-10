package database

import (
	"fmt"
	log "github.com/getwe/goose/log"
	. "github.com/getwe/goose/utils"
	"path/filepath"
	"sync"
)

const (
	// value文件最大不要超过512MB
	maxValueFileSize = 512 * 1024 * 1024
)

// data磁盘数据文件自描述所需的字段
type ValueManagerStatus struct {
	// 最大id
	MaxInId InIdType

	// 一个value的大小,单位(byte)
	ValueSize uint32
}

// 基于mmap的value管理.
// value设计为定长,在goose中value的长度应该在100字节以内比较好.
// 假设配置制定value定长32字节,1千万个文档占用mmap空间.
// 32*1000*10000 /1024/1024 = 306MB.
type ValueManager struct {
	JsonStatusFile

	// 磁盘存储目录
	filePath string
	// 磁盘同步操作锁
	lock sync.RWMutex

	// 每个文件存放value数量
	fileValueMaxCnt uint32

	// 文件数量
	fileCnt uint32

	// mmap文件数组
	mfile []MmapFile

	// 本身status
	valueStatus ValueManagerStatus
}

func (this *ValueManager) Open(path string) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.filePath = path

	// 磁盘状态文件需要设置的两个步骤:(1)指示要写入的结构;(2)设置写入路径
	this.SelfStatus = &this.valueStatus
	this.StatusFilePath = filepath.Join(this.filePath, "value.stat")
	err := this.ParseJsonFile()
	if err != nil {
		return err
	}

	this.fileValueMaxCnt = uint32(maxValueFileSize / this.valueStatus.ValueSize)
	this.fileCnt = uint32(uint32(this.valueStatus.MaxInId)/this.fileValueMaxCnt) + 1
	this.mfile = make([]MmapFile, this.fileCnt)

	// 分配磁盘空间
	for i := 0; uint32(i) < this.fileCnt; i++ {
		tname := fmt.Sprintf("value.n%d", i)
		sz := uint32(this.fileValueMaxCnt * this.valueStatus.ValueSize)
		err := this.mfile[i].OpenFile(path, tname, sz)
		if err != nil {
			return log.Error("open mfile[%d],szie[%d] fail[%s]", i, sz, err.Error())
		}
	}

	return nil
}

func (this *ValueManager) Init(path string, maxId InIdType, valueSz uint32) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.filePath = path

	this.valueStatus.MaxInId = maxId
	this.valueStatus.ValueSize = valueSz

	// 磁盘状态文件需要设置的两个步骤:(1)指示要写入的结构;(2)设置写入路径
	this.SelfStatus = &this.valueStatus
	this.StatusFilePath = filepath.Join(this.filePath, "value.stat")

	this.fileValueMaxCnt = uint32(maxValueFileSize / this.valueStatus.ValueSize)
	this.fileCnt = uint32(uint32(maxId)/this.fileValueMaxCnt) + 1
	this.mfile = make([]MmapFile, this.fileCnt)

	// 分配磁盘空间
	for i := 0; uint32(i) < this.fileCnt; i++ {
		tname := fmt.Sprintf("value.n%d", i)
		sz := uint32(this.fileValueMaxCnt * this.valueStatus.ValueSize)
		err := this.mfile[i].OpenFile(path, tname, sz)
		if err != nil {
			return log.Error("open mfile[%d],szie[%d] fail[%s]", i, sz, err.Error())
		}
	}

	return this.SaveJsonFile()
}

func (this *ValueManager) Sync() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	for i := 0; uint32(i) < this.fileCnt; i++ {
		err := this.mfile[i].Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

// 写入Value.可并发写
func (this *ValueManager) WriteValue(inId InIdType, v Value) error {
	if inId > this.valueStatus.MaxInId {
		return log.Error("inId [%d] illegal MaxInId[%d]", inId, this.valueStatus.MaxInId)
	}

	fileNo := uint32(int64(inId) / int64(this.fileValueMaxCnt))
	offset := uint32(int64(inId)%int64(this.fileValueMaxCnt)) * this.valueStatus.ValueSize

	if fileNo >= this.fileCnt {
		return log.Error("inId out of limit")
	}

	// 最多写入this.valueStatus.ValueSize个字节
	err := this.mfile[fileNo].WriteBytes(offset, v[:], this.valueStatus.ValueSize)
	if err != nil {
		return err
	}
	return nil
}

// 读取value的引用,value只能进行读操作,任何写操作都是非法的
func (this *ValueManager) ReadValue(inId InIdType) (Value, error) {
	if inId > this.valueStatus.MaxInId {
		return nil, log.Error("inId [%d] illegal MaxInId[%d]", inId, this.valueStatus.MaxInId)
	}

	fileNo := uint32(int64(inId) / int64(this.fileValueMaxCnt))
	offset := uint32(int64(inId)%int64(this.fileValueMaxCnt)) * this.valueStatus.ValueSize

	if fileNo >= this.fileCnt {
		return nil, log.Error("inId out of limit")
	}

	v, err := this.mfile[fileNo].ReadBytes(offset, this.valueStatus.ValueSize)
	if err != nil {
		return nil, err
	}
	return v[:], nil
}

func NewValueManager() *ValueManager {
	v := ValueManager{}

	return &v
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
