package database

import (
    . "github.com/getwe/goose/utils"
)

// 索引迭代器.
type IndexIterator interface {
    // 获取索引库中的下一个TermSign
    Next() (TermSign)
}

// 可合并只读索引库
type ReadOnlyIndex interface {
    // 创建遍历器
    NewIterator() (IndexIterator)

    // 索引读取接口
    ReadIndex(t TermSign)(*InvList,error)
}

// 可合并只写索引库
type WriteOnlyIndex interface {
    // 索引写接口
    WriteIndex(t TermSign,l *InvList) (error)
}

type IndexReader interface {
    ReadIndex(t TermSign)(*InvList,error)
}

type IndexWriter interface {
    // 写入索引
    WriteIndex(InID InIdType,termlist []TermInDoc)(error)
}

type ValueReader interface {
    // 读取Value
    ReadValue(InID InIdType) (Value,error)
}

type ValueWriter interface {
    // 写入Value
    WriteValue(InID InIdType,v Value) (error)
}

type DataReader interface {
    // 读取Data
    ReadData(inId InIdType,buf *Data) (error)
}

type DataWriter interface {
    // 写入Data
    WriteData(InID InIdType,d Data) (error)
}

// 读索引接口
type DataBaseReader interface {
    // 查询外部ID
    GetOutID(inId InIdType)(OutIdType,error)

    // 支持索引写入
    IndexReader

    // 支持Value读取
    ValueReader

    // 支持Data读取
    DataReader
}

// 可写入数据库接口
type DataBaseWriter interface {
    // 根据唯一外部ID,分配内部ID
    AllocID(outID OutIdType) (InIdType,error)

    // 支持索引写入
    IndexWriter

    // 支持Value写入
    ValueWriter

    // 支持Data写入
    DataWriter

    // 进行一次同步
    Sync()(error)
}








