package database

import (
	"testing"
    . "github.com/getwe/goose/utils"
    "os"
    "path/filepath"
)


func createList(sz int,weight int) (*InvList) {
    var lst InvList = make([]Index,0,sz)
    for i:=1;i < sz;i++ {
        lst = append(lst,Index{InIdType(i),TermWeight(weight)})
    }
    return &lst
}


func TestDiskIndex(t *testing.T){
    var testpath = filepath.Join(os.Getenv("HOME"),"hehe","tmp","goosedb")
    var maxId InIdType = 1*1000 // 100w
    var maxTermCnt int64 = int64(maxId) * 32 // 每个doc32个term
    var maxFileSz uint32 = 1024*1024*1024 // 1024MB

    {
        index := NewDiskIndex()
        t.Logf("maxFileSize : %d",maxFileSz)
        err := index.Init(testpath,"static",maxFileSz,maxTermCnt)
        if err != nil {
            t.Error("init index",err.Error())
            return
        }

        // 写入拉链
        var i int64
        for i=0;i < maxTermCnt/2;i++ {
            lst := createList(int(10+i),int(i))
            //t.Log(i,lst)
            err := index.WriteIndex(TermSign(i),lst)
            if err != nil {
                t.Error("write index",err.Error())
                return
            }
        }

        // 关闭索引
        index.Close()
    }
    t.Log("finish write,reopen to check")
    {
        // 再打开
        index := NewDiskIndex()
        err := index.Open(testpath,"static")
        if err != nil {
            t.Error("open index",err.Error())
            return
        }


        // 读索引
        var i int64
        for i=0;i < maxTermCnt/2;i++ {
            invlist,err := index.ReadIndex(TermSign(i))
            if err != nil {
                t.Error("read index",err.Error())
                return
            }

            //t.Log(i,invlist)

            // 校验读出来的拉链是正确的
            for j,e := range *invlist {
                if e.Weight != TermWeight(i) {
                    t.Errorf("term[%d] id[%d] [%d] != [%d]",i,j,e.Weight,i)
                    return
                }
            }
        }

    }
}









/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
