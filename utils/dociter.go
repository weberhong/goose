package utils

import (
    "bufio"
    "os"
)

type FileIter struct {
    s   *bufio.Scanner
}

// 内部进行了一次拷贝返回[]buf
func (this *FileIter) NextDoc() interface{} {
    if this.s.Scan() {
        ref := this.s.Bytes()
        buf := make([]byte,len(ref))
        copy(buf,ref)
        return buf
    }
    return nil
}

func NewFileIter(fh *os.File) (*FileIter) {
    fi := FileIter{}
    fi.s = bufio.NewScanner(fh)
    return &fi
}

