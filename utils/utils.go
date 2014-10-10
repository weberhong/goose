package utils

import (
	"bytes"
	"encoding/gob"
	"os"
	"reflect"
)

// 反射一个数字的底层类型以及占用内存大小(字节)
// 字节为0表示该类型不支持
func IntKindSize(n interface{}) (reflect.Kind, int) {
	realValue := reflect.ValueOf(n)
	t := realValue.Kind()
	switch t {
	case reflect.Int8, reflect.Uint8:
		return t, 1
	case reflect.Int16, reflect.Uint16:
		return t, 2
	case reflect.Int32, reflect.Uint32:
		return t, 4
	case reflect.Int64, reflect.Uint64:
		return t, 8
	}
	return 0, 0
}

// 把数据编码成二进制流,函数内部分配了内存.
// 经过实验得到,slice编码浪费的空间很大,一个int32类型的slice有这样的实验数据:
//  lst1 len[0] cap[5] --GobEncode--> buf1 len[18] cap[64]
//  lst2 len[3] cap[5] --GobEncode--> buf2 len[21] cap[64]
//  lst3 len[3] cap[10] --GobEncode--> buf3 len[21] cap[64]
//  lst4 len[64] cap[64] --GobEncode--> buf4 len[82] cap[197]
func GobEncode(data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 把二进制流反序列化
func GobDecode(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}

func FileSize(f *os.File) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func Ns2Ms(t int64) int64 {
	return t / 1000000
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
