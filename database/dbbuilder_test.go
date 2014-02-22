package database

import (
    . "github.com/getwe/goose/utils"
	"testing"
    "path/filepath"
    "os"
    "math/rand"
    "time"
    //"fmt"
)

//func createList(sz int32) (*InvList) {
//func createBigString(n int32,c byte) ([]byte) {
//func newData(n int32) ([]byte) {

func createTermlist(inid InIdType) []TermInDoc {
    lst := make([]TermInDoc,32)
    for i,_ := range lst {
        lst[i].Sign = TermSign(rand.Int63())
        lst[i].Weight = 100
    }
    return lst
}

func TestDBBuilder(t *testing.T){
    rand.Seed(time.Now().Unix())

    var testpath = filepath.Join(os.Getenv("HOME"),"hehe","tmp","goosedb")
    var transformMaxTermCnt = 40*10000
    var maxId InIdType = 2*10000 // 10w
    var maxFileSz uint32 = 1024*1024*1024 // 1024MB
    var valueSz uint32 = 32

    db := NewDBBuilder()
    err := db.Init(testpath,transformMaxTermCnt,maxId,valueSz,maxFileSz,maxFileSz)
    if err != nil {
        t.Error(err.Error())
        return
    }

    valueToPut := [...]byte{ 'a','b','c','d','e','f'}

    for i:=1;i<int(maxId);i++ {

        inId,err := db.AllocID(OutIdType(i))
        if err != nil {
            t.Error(err.Error())
            return
        }


        err = db.WriteData(inId,newData(int32(i)))
        if err != nil {
            t.Error(err.Error())
            return
        }

        err = db.WriteValue(inId,valueToPut[:])
        if err != nil {
            t.Error(err.Error())
            return
        }


        err = db.WriteIndex(inId,createTermlist(inId))
        if err != nil {
            t.Error(err.Error())
            return
        }

    }

    t1 := time.Now().Unix()

    db.Sync()

    t2 := time.Now().Unix()
    t.Logf("db.Sync() use time(s) : %d",t2-t1)
}
