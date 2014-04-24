package goose

import (
    "github.com/getwe/goose/config"
    . "github.com/getwe/goose/utils"
    . "github.com/getwe/goose/database"
    "runtime"
    log "github.com/getwe/goose/log"
    "net"
    "fmt"
    "sync"
)

// Goose检索程序.核心工作是提供检索服务,同时支持动态插入索引.
type GooseSearch struct {
    conf            config.Conf

    // 支持检索的db,同时提供动态插入索引功能
    searchDB        *DBSearcher

    // 动态索引生成器
    varIndexer      *VarIndexer

    // 检索流程
    searcher        *Searcher

}

func (this *GooseSearch) Run() error {

    // read conf
    searchGoroutineNum := this.conf.Int64("GooseSearch.Search.GoroutineNum")
    searchSvrPort := this.conf.Int64("GooseSearch.Search.ServerPort")
    indexSvrPort := this.conf.Int64("GooseSearch.Index.ServerPort")

    searchReqBufSize := this.conf.Int64("GooseSearch.Search.RequestBufferSize")
    searchResBufSize := this.conf.Int64("GooseSearch.Search.ResponseBufferSize")

    indexReqBufSize := this.conf.Int64("GooseSearch.Index.RequestBufferSize")
    //indexResBufSize := this.conf.GetInt("GooseSearch.Index.ResponseBufferSize")


    err := this.runSearchServer(int(searchGoroutineNum),int(searchSvrPort),
        int(searchReqBufSize),int(searchResBufSize))
    if err != nil {
        return err
    }

    err = this.runIndexServer(int(indexSvrPort),int(indexReqBufSize))
    if err != nil {
        return err
    }

    neverReturn := sync.WaitGroup{}
    neverReturn.Add(1)
    neverReturn.Wait()

    return nil
}

func (this *GooseSearch) runSearchServer(routineNum int,listenPort int,
    requestBufSize int,responseBufSize int) error {

    listener,err := net.Listen("tcp",fmt.Sprintf("localhost:%d",listenPort))
    if err != nil {
        log.Error("runSearchServer listen fail : %s",err.Error())
        return err
    }

    for i:=0;i<routineNum;i++ {
        go func() {
            reqbuf := make([]byte,requestBufSize)
            resbuf := make([]byte,responseBufSize)

            for {

                // clear buf
                reqbuf = reqbuf[:0]
                resbuf = resbuf[:0]

                conn,err := listener.Accept()
                if err != nil {
                    log.Warn("SearchServer accept fail : %s",err.Error())
                    goto LabelError
                }
                // receive data
                _,err = conn.Read(reqbuf)
                if err != nil {
                    log.Warn("SearchServer read fail : %s",err.Error())
                    goto LabelError
                }

                // do search
                err = this.searcher.Search(reqbuf,resbuf)
                if err != nil {
                    log.Warn("SearchServer Search fail : %s",err.Error())
                    goto LabelError
                }

                // write data
                _,err = conn.Write(resbuf)
                if err != nil {
                    log.Warn("SearchServer conn write fail : %s",err.Error())
                    goto LabelError
                }

                LabelError:
                conn.Close()
            }
        }()
    }
    return nil
}

func (this *GooseSearch) runIndexServer(listenPort int,requestBufSize int) error {

    if this.varIndexer == nil {
        return nil
    }

    listener,err := net.Listen("tcp",fmt.Sprintf("localhost:%d",listenPort))
    if err != nil {
        log.Error("runIndexServer listen fail : %s",err.Error())
        return err
    }

    // 简单一个协程完成接受请求和完成处理.索引更新不要求高并发性.
    go func() {
        reqbuf := make([]byte,requestBufSize)
        for {
            conn,err := listener.Accept()
            if err != nil {
                log.Warn("IndexServer accept fail : %s",err.Error())
                goto LabelError
            }

            // receive data
            _,err = conn.Read(reqbuf)
            if err != nil {
                log.Warn("IndexSearcher read fail : %s",err.Error())
                goto LabelError
            }

            // index
            err = this.varIndexer.BuildIndex(NewBufferIterOnce(reqbuf))
            if err != nil {
                log.Warn("IndexSearcher BuildIndex fail : %s",err.Error())
                goto LabelError
            }

            LabelError:
            conn.Close()
        }
    }()

    return nil
}

func (this *GooseSearch) Init(confPath string,
    indexSty IndexStrategy,searchSty SearchStrategy)(err error) {

    defer func() {
        if r := recover();r != nil {
            log.Error(r)
            str := r.(string)
            err = NewGooseError("GooseSearch.Init","Catch Exception",str)
        }
    }()

    // load conf
    this.conf,err = config.NewConf(confPath)
    if err != nil {
        return
    }

    // set max procs
    maxProcs := int(this.conf.Int64("GooseSearch.MaxProcs"))
    if maxProcs <= 0 {
        maxProcs = runtime.NumCPU()
    }
    runtime.GOMAXPROCS(maxProcs)
    log.Debug("set max procs [%d]",maxProcs)

    // init dbsearcher
    dbPath := this.conf.String("GooseBuild.DataBase.DbPath")
    log.Debug("init db [%s]",dbPath)

    this.searchDB = NewDBSearcher()
    err = this.searchDB.Init(dbPath)
    if err != nil {
        return
    }
    log.Debug("init db [%s]",dbPath)

    // index strategy global init
    if indexSty != nil {
        err = indexSty.Init(this.conf)
        if err != nil {
            return
        }
    }
    log.Debug("index strategy init finish")

    // search strategy global init
    if searchSty != nil {
        err = searchSty.Init(this.conf)
        if err != nil {
            return
        }
    }
    log.Debug("search strategy init finish")

    // var indexer
    if indexSty != nil {
        this.varIndexer,err = NewVarIndexer(this.searchDB,indexSty)
        if err != nil {
            return
        }
    }
    log.Debug("VarIndexer init finish")

    // searcher
    if searchSty != nil {
        this.searcher,err = NewSearcher(this.searchDB,searchSty)
        if err != nil {
            return
        }
    }
    log.Debug("Searcher init finish")

    return
 }


func NewGooseSearch() (*GooseSearch) {
    s := GooseSearch{}
    s.searchDB = nil
    s.searcher = nil
    s.varIndexer = nil
    return &s
}
