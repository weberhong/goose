package log

import (
    "fmt"
)

type GooseLogger struct {
    logstr []string
}

func NewGooseLogger() (*GooseLogger) {
    return &GooseLogger{}
}

func Warn(arg0 interface{}, args ...interface{}) error {
    return errorLogger.Warn(arg0,args)
}
// Warn日志直接输出
func (GooseLogger) Warn(arg0 interface{}, args ...interface{}) error {
    return Warn(arg0,args)
}

func Error(arg0 interface{}, args ...interface{}) error {
    return errorLogger.Error(arg0,args)
}
// Error日志直接输出
func (GooseLogger) Error(arg0 interface{}, args ...interface{}) error {
    return Error(arg0,args)
}

func Debug(arg0 interface{}, args ...interface{}) error {
    debugLogger.Debug(arg0,args)
    return nil
}
// Debug日志直接输出
func (GooseLogger) Debug(arg0 interface{}, args ...interface{}) error {
    Debug(arg0,args)
    return nil
}


// 直接使用Info日志,马上打印一行
func Info(arg0 interface{}, args ...interface{}) error {
    infoLogger.Info(arg0,args)
    return nil
}

// Info日志先存起来,调用PrintAllInfo的时候输出日志
func (this *GooseLogger) Info(format string, args ...interface{}) error {
    this.logstr = append(this.logstr,fmt.Sprintf(format,args...))
    return nil
}

// 输出全部Info日志
func (this *GooseLogger) PrintAllInfo() error {
    infoLogger.Info(this.logstr)
    this.logstr = this.logstr[:0]
    return nil
}


