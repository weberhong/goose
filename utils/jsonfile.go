package utils

import (
	"encoding/json"
    "io/ioutil"
)

func JsonEncodeToFile(v interface{},fullpath string) (error) {
    buf,err := json.MarshalIndent(v,"","  ")
    if err != nil {
        return err
    }
    return ioutil.WriteFile(fullpath,buf,0644)
}

func JsonDecodeFromFile(v interface{},fullpath string) (error) {
    buff,err := ioutil.ReadFile(fullpath)
    if err != nil {
        return err
    }
    return json.Unmarshal(buff,v)
}

// 磁盘json文件的序列化/反序列化小工具.
// 使用方法:(1)设置正确的statusFilePath;(2).设置selfStatus.
// 直接调用parseJsonFile和saveJsonFile
type JsonStatusFile struct {
    // 要写入磁盘的任意类型
    SelfStatus      interface{}
    // 写入磁盘的路径
    StatusFilePath  string
}
// 从磁盘解析数据
func (this *JsonStatusFile) ParseJsonFile() (error) {
    return JsonDecodeFromFile(&this.SelfStatus,this.StatusFilePath)
}

// 保存数据到磁盘
func (this *JsonStatusFile) SaveJsonFile() (error) {
    return JsonEncodeToFile(this.SelfStatus,this.StatusFilePath)
}







/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
