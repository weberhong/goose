package utils

import (
	"os"
	"testing"
)

var testpath = os.TempDir()
var testname = "babi"
var suggestFileSize uint32 = 1 * 1024

func createBigString(n int32, c byte) []byte {
	buf := make([]byte, n)
	for i, _ := range buf {
		buf[i] = c
	}
	return buf
}

func TestBigFile(t *testing.T) {

	big := BigFile{}
	big.Init(testpath, testname, suggestFileSize)
	big.Close()

	var testTime int = 1
	// 初始化,写入,退出,执行多次
	t.Logf("\n\n doTest No [%d]", testTime)
	testTime++
	lastStatInfo := openAndWriteSomeThing(t, nil)
	t.Logf("\n\n doTest No [%d]", testTime)
	testTime++
	lastStatInfo = openAndWriteSomeThing(t, &lastStatInfo)
	t.Logf("\n\n doTest No [%d]", testTime)
	testTime++
	lastStatInfo = openAndWriteSomeThing(t, &lastStatInfo)
}

func openAndWriteSomeThing(t *testing.T, lastRunTimeWriteInfo *BigFileStat) BigFileStat {

	big := BigFile{}
	err := big.Open(testpath, testname)
	if err != nil {
		t.Errorf("big.Open fail --- %s", err.Error())
		return BigFileStat{}
	}

	fileInfo := big.GetStatInfo()
	t.Logf("open file finish --- FileCnt[%d] LastFileOffset[%d] SuggestFileSize[%d]",
		fileInfo.FileCnt, fileInfo.LastFileOffset, fileInfo.SuggestFileSize)

	if suggestFileSize != fileInfo.SuggestFileSize {
		t.Errorf("suggestFileSize set[%d] get[%d]", suggestFileSize, fileInfo.SuggestFileSize)
		return BigFileStat{}
	}

	strSiz := suggestFileSize/3 + 1
	strCnt := 5
	var lastIndex *BigFileIndex
	for i := 0; i < strCnt; i++ {
		buf := createBigString(int32(strSiz), 'a'+byte(i))
		index, err := big.Append(buf)
		if err != nil {
			t.Errorf("big.Append fail --- %s", err.Error())
			return BigFileStat{}
		}

		t.Logf("big.Append succ --- FileNo[%d] offset[%d] length [%d]",
			index.FileNo, index.Offset, index.Length)

		// 文件已经存在
		// 当前这次启动,第一次写入的时候,检查是否跟着以前的文件末尾继续写入
		if lastRunTimeWriteInfo != nil && i == 0 {
			// 第一次写就开启了新文件了,那么必须必须是从文件头开始写
			if index.FileNo == lastRunTimeWriteInfo.FileCnt {
				if index.Offset != 0 {
					t.Errorf("Write new file not from offset 0")
					return BigFileStat{}
				}
			}
			// 还是跟着原来最后一个文件继续写,检查写起始位置是否正确
			if index.FileNo == lastRunTimeWriteInfo.FileCnt-1 {
				if index.Offset != lastRunTimeWriteInfo.LastFileOffset {
					t.Errorf("Write old file not from correct offset")
					return BigFileStat{}
				}
			}
		}

		lastIndex = index
	}

	fileInfo = big.GetStatInfo()
	if fileInfo.FileCnt != lastIndex.FileNo+1 {
		t.Errorf("FileCnt[%d] != lastIndex.FileNo[%d] + 1", fileInfo.FileCnt, lastIndex.FileNo)
		return BigFileStat{}
	}

	t.Logf("append file finish --- FileCnt[%d] LastFileOffset[%d] SuggestFileSize[%d]",
		fileInfo.FileCnt, fileInfo.LastFileOffset, fileInfo.SuggestFileSize)

	big.Close()

	return fileInfo
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
