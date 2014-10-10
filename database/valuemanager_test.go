package database

import (
	. "github.com/getwe/goose/utils"
	"os"
	"path/filepath"
	"testing"
)

func TestValueManagerInit(t *testing.T) {

	var testpath = filepath.Join(os.Getenv("HOME"), "hehe", "tmp", "goosedb", "test_valuemanager")

	os.RemoveAll(testpath)
	os.MkdirAll(testpath, 0755)

	t.Logf("open file [%s]", filepath.Join(testpath))

	var valueMgr ValueManager
	// 1kw个doc,每个64个字节value
	maxId := 1000 * 10000
	err := valueMgr.Init(testpath, InIdType(maxId), 16)
	if err != nil {
		t.Error(err.Error())
		return
	}

	// put value with a illegal id
	valueToPut := [...]byte{'a', 'b', 'c', 'd', 'e', 'f'}
	err = valueMgr.WriteValue(InIdType(maxId+3), valueToPut[:])
	if err != nil {
		t.Logf("expect error : %s", err.Error())
	} else {
		t.Errorf("use illegal inid without erroe")
	}
	// get value with a illegal id
	_, err = valueMgr.ReadValue(InIdType(maxId + 3))
	if err != nil {
		t.Logf("expect error : %s", err.Error())
	} else {
		t.Errorf("use illegal inid without erroe")
	}

	// put value with a legal id
	legalId := InIdType(30)
	valueToPut = [...]byte{'a', 'b', 'c', 'd', 'e', 'f'}
	err = valueMgr.WriteValue(legalId, valueToPut[:])
	if err != nil {
		t.Errorf("unexpect error : %s", err.Error())
	}
	// get value with a legal id
	valueGet, err := valueMgr.ReadValue(legalId)
	if err != nil {
		t.Errorf("unexpect error : %s", err.Error())
	}

	for i, v := range valueToPut {
		if valueGet[i] != v {
			t.Errorf("put != get")
		} else {
			t.Logf("get value[%d] = %c", i, valueGet[i])
		}
	}

	valueMgr.Sync()

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
