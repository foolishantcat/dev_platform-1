package net_server

import (
	"os"
	"sdk/logger"
	"strconv"
	"github.com/larspensjo/config"
)

type BindConf struct {
	ip_address string
	port       string
}

type Config struct {
	file                *os.File
	bind_conf           BindConf
	max_accpet_nums     int
	alram_time          int
	max_packet_size     int
	max_out_packet_nums int
}

func (this *Config) SetDefault() {

}

func (this *Config) SetServerAddr(ip_address string, port string) {

	this.bind_conf.ip_address = ip_address
	this.bind_conf.port = port
}

func (this *Config) SetAccpetNums(max_accpet_nums int) {
	this.max_accpet_nums = max_accpet_nums
}

func (this *Config) SetAlramTime(alram_time_sec int) {
	this.alram_time = alram_time_sec
}

func (this *Config) SetOutPacketNums(max_out_packet_nums int) {
	this.max_out_packet_nums = max_out_packet_nums
}

func (this *Config) SetPacketSize(max_packet_size int) {
	this.max_packet_size = max_packet_size
}

func (this *Config) LoadFile(file_path string) bool {
	// 读取的ini配置文件
	log_obj := logger.Instance()
	ini_conf, err := config.ReadDefault(file_path)
	if err != nil {
		log_obj.LogAppError("Ini File Err!ErrString=%s", err.Error())
		return false
	}
	
	var value string

	// host
	if !ini_conf.HasOption("listen", "host"){
		log_obj.LogAppError("Has No [listen.host] Section!")
		return false
	}
	this.bind_conf.ip_address, _ = ini_conf.String("listen", "host")

	// port
	if !ini_conf.HasOption("listen", "port") {
		log_obj.LogAppError("Has No [listen.port] Section!")
		return false
	}
	this.bind_conf.port, _ = ini_conf.String("listen", "port")

	// accpet_nums
	if !ini_conf.HasOption("listen", "max_accept_nums") {
		log_obj.LogAppError("Has No [listen.max_accpet_nums] Section!")
		return false
	}
	value, _= ini_conf.String("listen", "max_accpet_nums")
	this.max_accpet_nums, _ = strconv.Atoi(value)

	// timeout
	if !ini_conf.HasOption("listen", "timeout") {
		log_obj.LogAppError("Has No [listen.timeout] Section!")
		return false
	}
	value, _= ini_conf.String("listen", "timeout")
	this.alram_time, _ = strconv.Atoi(value)

	// packet_nums
	if !ini_conf.HasOption("listen", "packet_nums") {
		log_obj.LogAppError("Has No [listen.packet_nums] Section!")
		return false
	}
	value, _= ini_conf.String("listen", "packet_nums")
	this.max_out_packet_nums, _ = strconv.Atoi(value)

	// packet_size
	if !ini_conf.HasOption("listen", "packet_size") {
		log_obj.LogAppError("Has No [listen.packet_size] Section!")
		return false
	}
	value, _= ini_conf.String("listen", "packet_size")
	this.max_packet_size, _ = strconv.Atoi(value)

	return true
}

func (this Config) GetBinderAddr() string {
	return this.bind_conf.ip_address
}

func (this Config) GetBinderPort() string {
	return this.bind_conf.port
}
