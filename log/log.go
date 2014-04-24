// log的简单封装
// 我所期待的log接口(我在工作环境熟悉的)是这样的:
// 1. 日志分为多个文件,一个普通INFO日志,一个DEBUG,另外一个是WARN,ERROR,FATAL类型
// 2. INFO一般不采用调用一次打一行日志
// 3. INFO一般一次逻辑处理只打一行
// 4. 除了INFO之外,其它每调用一次,输出一行日志.
package log

import (
    log4go "github.com/alecthomas/log4go"
    config "github.com/getwe/goose/config"
)

var (
    debugLogger log4go.Logger
    infoLogger  log4go.Logger
    errorLogger log4go.Logger
)

func init() {
    debugLogger = make(log4go.Logger)
    infoLogger  = make(log4go.Logger)
    errorLogger = make(log4go.Logger)
}


func newFileFilter(file string) (*log4go.FileLogWriter) {
	flw := log4go.NewFileLogWriter(file, false)
	//flw.SetFormat("[%D %T] [%L] (%S) %M")
	flw.SetFormat("[%D %T] [%L] %M")
	flw.SetRotateLines(0)
	flw.SetRotateSize(0)
	flw.SetRotateDaily(false)

    return flw
}

func LoadConfiguration(confPath string) (error) {

    conf,err := config.NewConf(confPath)
    if err != nil {
        return err
    }

    var filt *log4go.FileLogWriter

    // debug
    debug_enable := conf.Bool("debug.Enable")
    debug_file := conf.String("debug.FileName")
    filt = nil
    if debug_enable {
        filt = newFileFilter(debug_file)
    }
    debugLogger["debug"] = &log4go.Filter{log4go.DEBUG, filt}

    // info
    info_enable := conf.Bool("info.Enable")
    info_file := conf.String("info.FileName")
    filt = nil
    if info_enable {
        filt = newFileFilter(info_file)
    }
    infoLogger["info"] = &log4go.Filter{log4go.INFO,filt}

    // error
    error_enable := conf.Bool("error.Enable")
    error_file := conf.String("error.FileName")
    filt = nil
    if error_enable {
        filt = newFileFilter(error_file)
    }
    errorLogger["error"] = &log4go.Filter{log4go.WARNING,filt}

    return nil
}

