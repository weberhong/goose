package goose

import (
    "fmt"
    "os"
    flags "github.com/jessevdk/go-flags"
)

// goose的入口程序.
type Goose struct {
    // 建库策略
    indexSty    IndexStrategy
    // 检索策略
    searchSty   SearchStrategy

    // 配置文件
    confPath    string

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
    // 解析命令行参数
    var opts struct {
        // build mode
        BuildMode bool `short:"b" long:"build" description:"run in build mode"`

        // configure file
        Configure string `short:"c" long:"conf" description:"congfigure file" required:"true"`

        // build mode data file
        DataFile string `short:"d" long:"datafile" description:"build mode data file"`
    }
    parser := flags.NewParser(&opts,flags.HelpFlag)
    _,err := parser.ParseArgs(os.Args)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

    this.confPath = opts.Configure

    if opts.BuildMode && len(opts.DataFile) == 0 {
        parser.WriteHelp(os.Stderr)
        os.Exit(1)
    }
    this.dataPath = opts.DataFile

    if opts.BuildMode {
        this.buildModeRun()
    } else {
        this.searchModeRun()
    }
}

// 建库模式运行
func (this *Goose) buildModeRun() {
    if this.indexSty == nil {
        // TODO FATAL LOG
        return
    }

    gooseBuild := NewGooseBuild()
    err := gooseBuild.Init(this.confPath,this.indexSty,this.dataPath)
    if err != nil {
        // TODO FATAL LOG
        return
    }

    err = gooseBuild.Run()
    if err != nil {
        // TODO FATAL LOG
        return
    }
}

// 检索模式运行
func (this *Goose) searchModeRun() {
    if this.searchSty == nil {
    }
    // TODO
}

func NewGoose() (*Goose) {
    g := Goose{}
    g.indexSty = nil
    g.searchSty = nil
    return &g
}
