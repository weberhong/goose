package goose

import (
    "fmt"
    "os"
    flags "github.com/jessevdk/go-flags"
    log "github.com/alecthomas/log4go"
    "time"
)

// goose的入口程序.
type Goose struct {
    // 建库策略
    indexSty    IndexStrategy
    // 检索策略
    searchSty   SearchStrategy

    // 配置文件
    confPath    string

    // 日志配置
    logConfPath    string

    // 建库模式数据文件
    dataPath    string
}

func (this *Goose) SetIndexStrategy(sty IndexStrategy) {
    this.indexSty = sty
}

func (this *Goose) SetSearchStrategy(sty SearchStrategy) {
    this.searchSty = sty
}

// 程序入口,解析程序参数,启动[建库|检索]模式
func (this *Goose) Run() {
    defer func() {
        if r := recover();r != nil {
            os.Exit(1)
        }
    }()

    // 解析命令行参数
    var opts struct {
        // build mode
        BuildMode bool `short:"b" long:"build" description:"run in build mode"`

        // configure file
        Configure string `short:"c" long:"conf" description:"congfigure file" default:"conf/goose.toml"`

        // log configure file
        LogConf   string `short:"l" long:"logconf" description:"log congfigure file" default:"conf/log.xml"`

        // build mode data file
        DataFile string `short:"d" long:"datafile" description:"build mode data file"`
    }
    parser := flags.NewParser(&opts,flags.HelpFlag)
    _,err := parser.ParseArgs(os.Args)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    if opts.BuildMode && len(opts.DataFile) == 0 {
        parser.WriteHelp(os.Stderr)
        os.Exit(1)
    }

    this.confPath = opts.Configure
    this.dataPath = opts.DataFile
    this.logConfPath = opts.LogConf

    // init log
    log.LoadConfiguration(this.logConfPath)
    log.Info("Load log conf finish")

    // run 
    if opts.BuildMode {
        this.buildModeRun()
    } else {
        this.searchModeRun()
    }


    // BUG(log4go) log4go need time to sync ...(wtf)
    // see http://stackoverflow.com/questions/14252766/abnormal-behavior-of-log4go
    time.Sleep(100 * time.Millisecond)
}

// 建库模式运行
func (this *Goose) buildModeRun() {

    if this.indexSty == nil {
        log.Critical("Please set index strategy,see Goose.SetIndexStrategy()")
        return
    }

    gooseBuild := NewGooseBuild()
    err := gooseBuild.Init(this.confPath,this.indexSty,this.dataPath)
    if err != nil {
        fmt.Println(err)
        log.Error(err)
        return
    }

    err = gooseBuild.Run()
    if err != nil {
        log.Error(err)
        return
    }

}

// 检索模式运行
func (this *Goose) searchModeRun() {

    if this.searchSty == nil {
        log.Critical("Please set search strategy,see Goose.SetSearchStrategy()")
        return
    }

    if this.indexSty == nil {
        log.Warn("can't build index real time witout Index Strategy")
    }

    gooseSearch := NewGooseSearch()
    err := gooseSearch.Init(this.confPath,this.indexSty,this.searchSty)
    if err != nil {
        log.Error(err)
        return
    }
}

func NewGoose() (*Goose) {
    g := Goose{}
    g.indexSty = nil
    g.searchSty = nil
    return &g
}
