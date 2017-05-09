package net_server

import (
	"os"
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
	return true
}
