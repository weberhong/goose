package database

import (
	. "github.com/getwe/goose/utils"
	"testing"
)

func createTermInDoc(base uint8) []TermInDoc {
	td := make([]TermInDoc, 0)
	td = append(td, TermInDoc{TermSign(100 + base + 1), TermWeight(base + 100 + 1)})
	td = append(td, TermInDoc{TermSign(100 + base + 2), TermWeight(base + 100 + 2)})
	td = append(td, TermInDoc{TermSign(100 + base + 3), TermWeight(base + 100 + 3)})
	return td
}

type printdb struct {
	T *testing.T
}

func (this printdb) WriteIndex(t TermSign, l *InvList) error {
	this.T.Log(t, l)
	return nil
}

func TestIndexTransform(t *testing.T) {
	tf := NewIndexTransform(100)
	tf.AddOneDoc(InIdType(1), createTermInDoc(1))
	tf.AddOneDoc(InIdType(2), createTermInDoc(2))
	tf.AddOneDoc(InIdType(3), createTermInDoc(3))
	tf.AddOneDoc(InIdType(4), createTermInDoc(30))

	tf.Dump(printdb{t})

	tf.Dump(printdb{t})
}
