package database

import (
	"testing"
    . "github.com/getwe/goose/utils"
    "os"
    "path/filepath"
    "fmt"
)


var testpath = filepath.Join(os.Getenv("HOME"),"hehe","tmp","goosedb","test_datamanager")
var maxId InIdType = 10*10000 // 100w
var maxFileSz uint32 = 1024*1024*1024 // 1024MB
var docSize int32 = 2*1024 // 5k


func TestDataManagerInit(t *testing.T){
    os.RemoveAll(testpath)
    os.MkdirAll(testpath,0755)

    openInitAppendData(t)
    openInitReadData(t)
}


func createBigString(n int32,c byte) ([]byte) {
    buf := make([]byte,n)
    for i,_ := range buf {
        buf[i] = c
    }
    return buf
}

func newData(n int32) ([]byte) {
    return createBigString(docSize,'a' + byte(n%26))
}
func checkData(n int32,dataBuf Data) (string) {
    if int32(len(dataBuf)) != docSize {
        return fmt.Sprintf("dm.Read --- %s","len(dataBuf) != docSize")
    }

    c := 'a' + byte(n%26)
    for i,v := range dataBuf {
        if v != c {
            return fmt.Sprintf("id[%d] -- [%d] read data [%c] != [%c]",n,i,v,c)
        }
    }
    return ""
}

func openInitAppendData(t *testing.T) {

    var dm DataManager = DataManager{}
    err := dm.Init(testpath,maxId,maxFileSz)
    if err != nil {
        t.Errorf("dm.Init --- %s",err.Error())
        return
    }

    var i InIdType
    for i=0;i<maxId;i++ {
        err := dm.Append(i,newData(int32(i)))
        if err != nil {
            t.Errorf("dm.Append --- %s",err.Error())
            break
        }
    }

    var dataBuf Data = make([]byte,docSize)
    for i=0;i<maxId;i++ {
        err := dm.ReadData(i,&dataBuf)
        if err != nil {
            t.Errorf("openInitReadData --- %s",err.Error())
            break
        }
        str := checkData(int32(i),dataBuf)
        if len(str) > 0 {
            t.Errorf("openInitReadData" + str)
            break
        }
    }


    dm.Sync()
    dm.Close()
}

func openInitReadData(t *testing.T) {

    var dm DataManager = DataManager{}
    err := dm.Open(testpath)
    if err != nil {
        t.Errorf("dm.Init --- %s",err.Error())
        return
    }

    var i InIdType
    var dataBuf Data = make([]byte,docSize)
    for i=0;i<maxId;i++ {
        err := dm.ReadData(i,&dataBuf)
        if err != nil {
            t.Errorf("openInitReadData --- %s",err.Error())
            break
        }
        str := checkData(int32(i),dataBuf)
        if len(str) > 0 {
            t.Errorf("openInitReadData" + str)
            break
        }
    }

    dm.Sync()
    dm.Close()
}





/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
