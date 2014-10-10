package database

import (
	. "github.com/getwe/goose/utils"
	"math"
)

// 两路合并索引库A/B,写入到索引库C.外部调用IndexMerge应该是指针传递进来
func IndexMerge(indexA, indexB ReadOnlyIndex, indexC WriteOnlyIndex) error {

	iterA := indexA.NewIterator()
	iterB := indexB.NewIterator()

	termA := iterA.Next()
	termB := iterB.Next()

	for termA != TermSign(0) && termB != TermSign(0) {
		if termA < termB {
			currTerm := termA
			termA = iterA.Next()

			lst, err := indexA.ReadIndex(currTerm)
			if err != nil {
				// TODO warnning log
				continue
			}
			err = indexC.WriteIndex(currTerm, lst)
			if err != nil {
				// TODO warnning log
				continue
			}
		} else if termA > termB {
			currTerm := termB
			termB = iterB.Next()

			lst, err := indexB.ReadIndex(currTerm)
			if err != nil {
				// TODO warnning log
				continue
			}
			err = indexC.WriteIndex(currTerm, lst)
			if err != nil {
				// TODO warnning log
				continue
			}
		} else {
			currTerm := termA

			termA = iterA.Next()
			termB = iterB.Next()

			lstA, err1 := indexA.ReadIndex(currTerm)
			lstB, err2 := indexB.ReadIndex(currTerm)
			if err1 != nil || err2 != nil {
				// TODO
				continue
			}
			lstA.Merge(*lstB, math.MaxInt32)

			err := indexC.WriteIndex(currTerm, lstA)
			if err != nil {
				// TODO
				continue
			}
		}
	}

	for ; termA != TermSign(0); termA = iterA.Next() {
		lst, err := indexA.ReadIndex(termA)
		if err != nil {
			// TODO warnning log
			continue
		}
		err = indexC.WriteIndex(termA, lst)
		if err != nil {
			// TODO warnning log
			continue
		}
	}

	for ; termB != TermSign(0); termB = iterB.Next() {
		lst, err := indexB.ReadIndex(termB)
		if err != nil {
			// TODO warnning log
			continue
		}
		err = indexC.WriteIndex(termB, lst)
		if err != nil {
			// TODO warnning log
			continue
		}
	}

	return nil
}
