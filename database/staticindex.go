package database

import (
	. "github.com/getwe/goose/utils"
)

// StaticIndex是静态索引的读取接口.只允许打开磁盘已存在的索引后进行读操作.
type StaticIndex struct {
	// 目前的设计,静态索引只由一个磁盘索引构成
	disk *DiskIndex
}

// 打开已存在的磁盘索引
func (this *StaticIndex) Open(path string) error {
	return this.disk.Open(path, "static")
}

// 读取索引
func (this *StaticIndex) ReadIndex(t TermSign) (*InvList, error) {
	return this.disk.ReadIndex(t)
}

// StaticIndex构造函数
func NewStaticIndex() *StaticIndex {
	s := StaticIndex{}
	s.disk = NewDiskIndex()
	return &s
}
