package utils

import (
	"bufio"
	"os"
)

type FileIter struct {
	s *bufio.Scanner
}

// 内部进行了一次拷贝返回[]buf
func (this *FileIter) NextDoc() interface{} {
	if this.s.Scan() {
		ref := this.s.Bytes()
		buf := make([]byte, len(ref))
		copy(buf, ref)
		return buf
	}
	return nil
}

func NewFileIter(fh *os.File) *FileIter {
	fi := FileIter{}
	fi.s = bufio.NewScanner(fh)
	return &fi
}

// 把一块buf当成一个doc一次返回
type BufferIterOnce struct {
	buf []byte
}

func (this *BufferIterOnce) NextDoc() interface{} {
	if this.buf != nil {
		tmp := this.buf
		this.buf = nil
		return tmp
	}
	return nil
}

func NewBufferIterOnce(buf []byte) *BufferIterOnce {
	bi := BufferIterOnce{}
	bi.buf = buf
	return &bi
}
