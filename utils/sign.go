package utils

import (
    "crypto/md5"
    "io"
    "encoding/binary"
)


// 字符串签名为数字,使用md5结果对折.
func StringSignMd5(text string) int64 {
    // TODO 目前还没测试该算法的冲突率.
    h := md5.New()
    io.WriteString(h,text)
    md5res := h.Sum(nil)
    order := binary.BigEndian
    sign1 := order.Uint64(md5res[0:8])
    sign2 := order.Uint64(md5res[8:16])

    return int64(sign1) + int64(sign2)
 }


 // 字符串签名为数字,使用BKDRHash算法.
 func StringSignBKDR(text string) int64 {
    // 代码参考:
    // https://www.byvoid.com/blog/string-hash-compare
     var seed int64 = 131   // 31 131 1313 13131 131313 etc..
     var hash int64

     for _,c := range text {
         hash = hash * seed + int64(c)
     }

     return hash
 }

