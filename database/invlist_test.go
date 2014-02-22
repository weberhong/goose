package database

import (
    . "github.com/getwe/goose/utils"
    "math"
	"testing"
    "time"
    "math/rand"
)

func TestInvList(t *testing.T) {
    lst := NewInvList()
    lst.Append(Index{1,101})
    lst.Append(Index{2,102})
    lst.Append(Index{3,103})

    if lst.Len() != 3 {
        t.Errorf("append error")
    }
    t.Log(lst)
}

func TestInvListKMerge(t *testing.T) {
    rand.Seed(time.Now().Unix())

    alllst := make([](*InvList),0)
    // 多个拉链
    for i:=1;i<5;i++ {
        lst := NewInvList()

        // 每个拉链多个元素
        lstLen := int(rand.Uint32() % 5)
        for j:=0;j<lstLen;j++ {
            lst.Append(Index{InIdType(i+j*10),TermWeight(i+j*10+500)})
        }
        alllst = append(alllst,&lst)
    }
    t.Log(alllst)

    klst := NewInvList()
    // 多路归并算法
    klst.KMerge(alllst,math.MaxInt32)
    t.Log(klst)

    // 校验一下是否有序
    var last InIdType = 0
    for _,e := range klst {
        if e.InID < last {
            t.Error("kmerge fail")
        }
        last = e.InID
    }
}

func TestInvListMerge(t *testing.T) {
    lst1 := NewInvList()
    lst1.Append(Index{1,101})
    lst1.Append(Index{3,103})
    lst1.Append(Index{5,105})
    lst1.Append(Index{7,107})

    lst2 := NewInvList()
    lst2.Append(Index{2,102})
    lst2.Append(Index{4,104})
    lst2.Append(Index{6,106})
    lst2.Append(Index{8,108})

    alllst := make([](*InvList),0)
    alllst = append(alllst,&lst1)
    alllst = append(alllst,&lst2)
    klst := NewInvList()
    klst.KMerge(alllst,math.MaxInt32)
    t.Log("多路归并算法归并多个拉链")
    t.Log(klst)

    onelst := make([](*InvList),0)
    onelst = append(onelst,&lst1)
    k1lst := NewInvList(0)
    k1lst.KMerge(onelst,math.MaxInt32)
    t.Log("多路归并算法归并一个拉链")
    t.Log(k1lst)
    for i:=0;i<lst1.Len();i++ {
        if lst1[i] != k1lst[i] {
            t.Error("merge error")
        }
    }


    lst1.Merge(lst2,math.MaxInt32)
    t.Log("两路归并算法")
    t.Log(lst1)

    if lst1.Len() != 8 {
        t.Error("lst merge fail")
        return
    }
    if klst.Len() != 8 {
        t.Error("lst kmerge fail")
        return
    }

    // 检查两个算法归并结果是否相同
    for i:=0;i<lst1.Len();i++ {
        if lst1[i] != klst[i] {
            t.Error("merge error")
        }
    }

}





/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
