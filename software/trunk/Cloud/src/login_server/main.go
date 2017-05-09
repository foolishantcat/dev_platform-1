package main

import (
	"fmt"
	"logger"
	"net_server"
)

var (
	login Login
)

func main() {
	log_obj := logger.Instance()
	err := log_obj.Load("../conf/conf.xml")
	if err != nil {
		fmt.Println("Load Failed!ErrString=%s", err.Error())
		return
	}

	defer log_obj.Close()
	var svr_conf net_server.Config

	if !svr_conf.LoadFile("../conf/server.json") {
		log_obj.LogAppError("Load Server Conf Failed!")
		return
	}

	server := net_server.CreateNetServer(&login)
	if !server.Start(&svr_conf) {
		log_obj.LogAppDebug("Start Server Failed!")
		return
	}
}
