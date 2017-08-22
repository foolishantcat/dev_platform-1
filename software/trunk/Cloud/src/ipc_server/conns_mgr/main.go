package main

import (
	"fmt"
	"sdk/logger"
)

var logic_obj ServerLogic

func main() {
	// 加载日志模块
	log_obj := logger.Instance()
	err := log_obj.Load("./conf/conf.xml")
	if err != nil {
		fmt.Printf("Init Log Module Failed!EttString=%s\n", err.Error())
		return
	}

	logic_obj.Initialize()
}
