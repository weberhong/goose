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
    return errorLogger.Warn(fmt.Sprintf("%s %s",getLineInfo(3),arg0),args...)
}
func Warn(arg0 interface{}, args ...interface{}) error {
    return _warn(arg0,args...)
}
func (GooseLogger) Warn(arg0 interface{}, args ...interface{}) error {
    return _warn(arg0,args...)
}

// Error日志直接输出
func _error(arg0 interface{}, args ...interface{}) error {
    return errorLogger.Error(fmt.Sprintf("%s %s",getLineInfo(3),arg0),args...)
}
func Error(arg0 interface{}, args ...interface{}) error {
    return _error(arg0,args...)
}
func (GooseLogger) Error(arg0 interface{}, args ...interface{}) error {
    return _error(arg0,args...)
}

// Debug日志直接输出
func _debug(arg0 interface{}, args ...interface{}) error {
    debugLogger.Debug(fmt.Sprintf("%s %s",getLineInfo(3),arg0),args...)
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
    infoLogger.Info(fmt.Sprintf("%s %s",getLineInfo(3),arg0),args...)
    return nil
}
func Info(arg0 interface{}, args ...interface{}) error {
    _info(arg0,args...)
    return nil
}

// Info日志先存起来,调用PrintAllInfo的时候输出日志
// 支持日常用法
// Info(object,xxx) : 输出一个对象的字符串化表示,忽略后面的参数
// Info(string) : 直接输出
// Info(strA,strB) : 输出strA:strB
// Info(strA,object) : 输出strA : object.string()
func (this *GooseLogger) Info(arg ...interface{}) error {

    var result string

    if len(arg) <= 1 {
        result = fmt.Sprintf("%s",arg)
    } else if len(arg) == 2 {
        result = fmt.Sprint(arg[0]) + ":" + fmt.Sprint(arg[1])
    } else {
        result = fmt.Sprint(arg[0]) + ":" + fmt.Sprint(arg[1:])
    }

    this.logstr = append(this.logstr,result)
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

