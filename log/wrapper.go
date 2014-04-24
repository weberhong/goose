package log

import (
    "fmt"
    "runtime"
    "strings"
)

type GooseLogger struct {
    logstr []string
}

func NewGooseLogger() (*GooseLogger) {
    return &GooseLogger{}
}

// Warn日志直接输出
func _warn(arg0 interface{}, args ...interface{}) error {
    format,ok := arg0.(string)
    if ok {
        return errorLogger.Warn(fmt.Sprintf("%s %s",getLineInfo(3),format),args...)
    }
    return nil
}
func Warn(arg0 interface{}, args ...interface{}) error {
    return _warn(arg0,args...)
}
func (GooseLogger) Warn(arg0 interface{}, args ...interface{}) error {
    return _warn(arg0,args...)
}

// Error日志直接输出
func _error(arg0 interface{}, args ...interface{}) error {
    format,ok := arg0.(string)
    if ok {
        return errorLogger.Error(fmt.Sprintf("%s %s",getLineInfo(3),format),args...)
    }
    return nil
}
func Error(arg0 interface{}, args ...interface{}) error {
    return _error(arg0,args...)
}
func (GooseLogger) Error(arg0 interface{}, args ...interface{}) error {
    return _error(arg0,args...)
}

// Debug日志直接输出
func _debug(arg0 interface{}, args ...interface{}) error {
    format,ok := arg0.(string)
    if ok {
        debugLogger.Debug(fmt.Sprintf("%s %s",getLineInfo(3),format),args...)
    }
    return nil
}
func Debug(arg0 interface{}, args ...interface{}) error {
    _debug(arg0,args...)
    return nil
}
func (GooseLogger) Debug(arg0 interface{}, args ...interface{}) error {
    _debug(arg0,args...)
    return nil
}


// 直接使用Info日志,马上打印一行
func _info(arg0 interface{}, args ...interface{}) error {
    format,ok := arg0.(string)
    if ok {
        infoLogger.Info(fmt.Sprintf("%s %s",getLineInfo(3),format),args...)
    }
    return nil
}
func Info(arg0 interface{}, args ...interface{}) error {
    _info(arg0,args...)
    return nil
}

// Info日志先存起来,调用PrintAllInfo的时候输出日志
func (this *GooseLogger) Info(format string, args ...interface{}) error {
    this.logstr = append(this.logstr,fmt.Sprintf(format,args...))
    return nil
}

// 输出全部Info日志
func (this *GooseLogger) PrintAllInfo() error {
    infoLogger.Info(strings.Join(this.logstr," "))
    this.logstr = this.logstr[:0]
    return nil
}

func getLineInfo(level int) string {
    pc, _, lineno, ok := runtime.Caller(level)
    if ok {
        return fmt.Sprintf("[%s:%d]", runtime.FuncForPC(pc).Name(), lineno)
    }
    return "[]"
}

