package goose

import (
	"github.com/getwe/goose/config"
	. "github.com/getwe/goose/database"
	log "github.com/getwe/goose/log"
	. "github.com/getwe/goose/utils"
	"os"
	"runtime"
)

// Goose的静态库生成程序.
type GooseBuild struct {
	conf config.Conf

	staticDB *DBBuilder

	staticIndexer *StaticIndexer

	fileHd   *os.File
	fileIter *FileIter
}

func (this *GooseBuild) Run() (err error) {
	defer this.fileHd.Close()

	// build index
	err = this.staticIndexer.BuildIndex(this.fileIter)
	if err != nil {
		return err
	}

	// db sync
	err = this.staticDB.Sync()
	if err != nil {
		return err
	}

	return nil
}

// 根据配置文件进行初始化.
// 需要外部指定索引策略,策略可以重新设计.
// 需要外部知道被索引文件(这个易变信息不适合放配置)
func (this *GooseBuild) Init(confPath string, indexSty IndexStrategy, toIndexFile string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = log.Error(r)
		}
	}()

	// load conf
	this.conf, err = config.NewConf(confPath)
	if err != nil {
		return
	}

	// set max procs
	maxProcs := int(this.conf.Int64("GooseBuild.MaxProcs"))
	if maxProcs <= 0 {
		maxProcs = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(maxProcs)

	// init dbbuilder
	dbPath := this.conf.String("GooseBuild.DataBase.DbPath")
	transformMaxTermCnt := this.conf.Int64("GooseBuild.DataBase.TransformMaxTermCnt")
	maxId := this.conf.Int64("GooseBuild.DataBase.MaxId")
	maxIndexFileSize := this.conf.Int64("GooseBuild.DataBase.MaxIndexFileSize")
	maxDataFileSize := this.conf.Int64("GooseBuild.DataBase.MaxDataFileSize")
	valueSize := this.conf.Int64("GooseBuild.DataBase.ValueSize")

	this.staticDB = NewDBBuilder()
	err = this.staticDB.Init(dbPath, int(transformMaxTermCnt), InIdType(maxId),
		uint32(valueSize), uint32(maxIndexFileSize), uint32(maxDataFileSize))
	if err != nil {
		return
	}

	// index strategy global init
	err = indexSty.Init(this.conf)
	if err != nil {
		return
	}

	// static indexer
	this.staticIndexer, err = NewStaticIndexer(this.staticDB, indexSty)
	if err != nil {
		return
	}

	// open data file
	this.fileHd, err = os.OpenFile(toIndexFile, os.O_RDONLY, 0644)
	if err != nil {
		return
	}

	// file iter
	this.fileIter = NewFileIter(this.fileHd)

	return nil
}

func NewGooseBuild() *GooseBuild {
	bui := GooseBuild{}
	return &bui
}
