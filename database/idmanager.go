package database

import (
	"encoding/binary"
	log "github.com/getwe/goose/log"
	. "github.com/getwe/goose/utils"
	"path/filepath"
	"sync"
)

const (
	// id都用int32表示,每个id使用4个字节存储
	idSize = 4
)

// id磁盘数据文件自描述所需的字段
type IdManagerStatus struct {
	// 当前未分配id
	CurId InIdType

	// 最大id
	MaxInId InIdType
}

// 在goose设计解决百万级别的doc数.
// 1千万个文档id,每个id使用4个字节存储 1000*10000*4/1024/1024 = 38MB .
// 索引量达到一千万,所有id的存储用一个文件就可以存储.
type IdManager struct {
	JsonStatusFile

	// 磁盘存储目录
	filePath string
	// 磁盘同步操作锁
	lock sync.RWMutex
	// mmap文件
	mfile MmapFile

	// 本身status
	idStatus IdManagerStatus
}

// path:工作目录.
func (this *IdManager) Open(path string) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.filePath = path

	// 磁盘状态文件需要设置的两个步骤:(1)指示要写入的结构;(2)设置写入路径
	this.SelfStatus = &this.idStatus
	this.StatusFilePath = filepath.Join(this.filePath, "id.stat")
	err := this.ParseJsonFile()
	if err != nil {
		return err
	}

	err = this.mfile.OpenFile(path, "id", uint32(this.idStatus.MaxInId*idSize))
	if err != nil {
		return err
	}

	return nil
}

// path:工作目录.
// maxid:内部id最大上限
func (this *IdManager) Init(path string, maxId InIdType) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	// 内部id为0认为非法
	this.idStatus.CurId = 1
	this.filePath = path

	this.idStatus.MaxInId = maxId
	// 磁盘状态文件需要设置的两个步骤:(1)指示要写入的结构;(2)设置写入路径
	this.SelfStatus = &this.idStatus
	this.StatusFilePath = filepath.Join(this.filePath, "id.stat")

	err := this.mfile.OpenFile(path, "id", uint32(maxId*idSize))
	if err != nil {
		return err
	}

	return this.SaveJsonFile()
}

// mmap内存数据需要定时同步到磁盘
func (this *IdManager) Sync() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.SaveJsonFile()

	err := this.mfile.Flush()
	return err
}

func (this *IdManager) Close() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.SaveJsonFile()

	return this.mfile.Close()
}

// 分配内部id
func (this *IdManager) AllocID(outId OutIdType) (InIdType, error) {
	this.lock.Lock()
	defer this.lock.Unlock()

	if outId == 0 {
		return 0, log.Warn("illegal outId [%d]", 0)
	}

	if this.idStatus.CurId >= this.idStatus.MaxInId {
		return 0, log.Error("InId [%d] out of limit MaxInId[%d]", this.idStatus.CurId, this.idStatus.MaxInId)
	}

	inID := this.idStatus.CurId

	// 分配信息,写入mmap
	offset := inID * idSize
	err := this.mfile.WriteNum(uint32(offset), uint32(outId))
	if err != nil {
		return 0, err
	}

	// 确认分配成功才真正占用这个id
	this.idStatus.CurId++

	return inID, nil
}

// 获取外部id
func (this *IdManager) GetOutID(inId InIdType) (OutIdType, error) {
	// 获取外部id只读操作,不需要加锁
	// 这是一个高并发操作,加锁性能就变差了
	//this.lock.Lock()
	//defer this.lock.Unlock()

	if inId >= this.idStatus.MaxInId {
		return 0, log.Error("inId [%d] illegal MaxInId[%d]", inId, this.idStatus.MaxInId)
	}

	offset := inId * idSize
	var outId OutIdType
	// FIXME 把内部外部id定义个新的类型反而更蛋疼的了,遇到这种反射的代码
	tmp, err := this.mfile.ReadNum(uint32(offset),
		uint32(binary.Size(outId)))
	if err != nil {
		return 0, err
	}
	outId = OutIdType(tmp)

	return outId, nil
}

func NewIdManager() *IdManager {
	id := IdManager{}

	return &id
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
