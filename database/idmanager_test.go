package database

import (
	"testing"
    . "github.com/getwe/goose/utils"
    "os"
    "path/filepath"
)

func TestIdManager(t *testing.T){
    var testpath = filepath.Join(os.Getenv("HOME"),"hehe","tmp","goosedb","test_idmanager")

    os.RemoveAll(testpath)
    os.MkdirAll(testpath,0755)

    t.Logf("open file [%s]",testpath)

    var idMgr IdManager
    err := idMgr.Init(testpath,1000*10000)
    if err != nil {
        t.Error(err.Error())
        return
    }

    // test alloc
    for i:=0;i<1000*10000;i++ {
        _,err := idMgr.AllocID(OutIdType(i))
        if err != nil {
            t.Error(err.Error())
            return
        }
    }

    for i:=0;i<1000*10000;i++ {
        ouId,err := idMgr.GetOutID(InIdType(i))
        if err != nil {
            t.Error(err.Error())
            return
        }
        if int32(ouId) != int32(i) {
            t.Errorf("GetOutID Fail InID[%d],OutID[%d]",i,ouId)
            return
        }
    }
    idMgr.Sync()
    idMgr.Close()


    // open read
    idMgr2 := NewIdManager()
    err = idMgr2.Open(testpath)
    if err != nil {
        t.Error(err.Error())
        return
    }
    for i:=0;i<1000*10000;i++ {
        ouId,err := idMgr2.GetOutID(InIdType(i))
        if err != nil {
            t.Error(err.Error())
            return
        }
        if int32(ouId) != int32(i) {
            t.Errorf("GetOutID Fail InID[%d],OutID[%d]",i,ouId)
            return
        }
    }
}








/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
