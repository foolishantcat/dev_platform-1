package main

import (
	"time"
	"fmt"
	"sdk/logger"
	"sdk/net_server"
)

var logic_obj ServerLogic

func main() {
	var conf net_server.Config

	// 加载日志模块
	log_obj := logger.Instance()
	err := log_obj.Load("../conf/log.xml")
	if  != nil {
		fmt.Printf("Init Log Module Failed!EttString=%s\n", err.Error())
		return
	}

	if !conf.LoadFile("../conf/server.json") {
		fmt.Printf("Load Config File Failed!")
		return
	}

	ipc_server_obj := net_server.CreateNetServer(&logic_obj)
	if !ipc_server_obj.Start(&conf) {
		logger.LogAppErr("ipc_server Start Failed!")
	}

	for{
		time.Sleep(time.Now().Second() + 10 * time.Second)
		
		// 不做任何处理
	}
}
