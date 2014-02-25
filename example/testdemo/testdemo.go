package main

import (
    "github.com/getwe/goose"
    "github.com/getwe/goose/strategy/testdemo"
)


func main() {

    app := goose.NewGoose()
    app.SetIndexStrategy(new(testdemo.StyIndexer))
    app.Run()
}
