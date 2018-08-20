package net_server

import (
	"net"
	"sdk/logger"
	"time"
)

type INetFunc interface {
	OnNetAccpet(peer_ip string) (bool, func(buf []byte) ([]byte, int, int))
	OnNetRecv(client INetClient, packet_buf []byte, params interface{})
	OnNetErr(client INetClient, params interface{})
}

type INetServer interface {
	AddParams(param interface{})
	Start(config *Config) bool
	Stop()
}

type clientConfig struct {
	max_packet_size     int
	max_out_packet_nums int
	alram_time          int
}
type netServer struct {
	local_ip        string
	port            string
	client_conf     clientConfig
	params_array    []interface{}
	max_accpet_nums int
	has_accpet_nums int
	net_func        INetFunc
	listen_chan     chan netEvent // 用来停止监听协程的
	logger          logger.ILogger
}

const (
	NET_CLOSE = 0
	NET_SEND  = 1
)

type netEvent struct {
	event_type int
	data_buf   []byte
}

func CreateNetServer(net_func INetFunc) INetServer {
	net_server := new(netServer)
	net_server.logger = logger.Instance()
	net_server.net_func = net_func
	net_server.params_array = make([]interface{}, 0, 10)

	return net_server
}

func (this *netServer) AddParams(params interface{}) {
	this.params_array = append(this.params_array, params)
}

func (this *netServer) Start(config *Config) bool {
	this.local_ip = config.bind_conf.ip_address
	this.port = config.bind_conf.port
	this.client_conf.alram_time = config.alram_time
	this.client_conf.max_out_packet_nums = config.max_out_packet_nums
	this.client_conf.max_packet_size = config.max_packet_size
	this.max_accpet_nums = config.max_accpet_nums
	this.has_accpet_nums = 0

	var netAddr string
	if this.local_ip != "0:0:0:0" {
		netAddr = this.local_ip
	}
	netAddr += ":"
	netAddr += this.port

	this.logger.LogSysInfo("Server Config Data!BindAddress=%s,TimeOut=%d,MaxPacketNums=%d,MaxPacketSize=%d,MaxAccpetNums=%d",
		netAddr,
		this.client_conf.alram_time,
		this.client_conf.max_out_packet_nums,
		this.client_conf.max_packet_size,
		this.max_accpet_nums)

	listen, err := net.Listen("tcp4", netAddr)
	if err != nil {
		this.logger.LogSysFatal("BindIP=%s,Port=%sErrString=%s",
			this.local_ip,
			this.port,
			err.Error())

		return false

	}
	this.listenRounte(listen)
	return true
}

func (this *netServer) listenRounte(listen net.Listener) {
	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			this.logger.LogSysError("Listen Failed!ErrString=%s", err.Error())
			return

		} else {
			// 创建netClient
			contxt, ok, parser := this.net_func.OnNetAccpet(conn.RemoteAddr().String())
			if ok {
				createNetClient(&this.client_conf, this.net_func, conn, contxt, parser)
			} else {
				this.logger.LogSysInfo("Remote Client Closed!PeerIP=%s", conn.RemoteAddr().String())
				conn.Close()
			}

		}
	}
}

func (this *netServer) Stop() {

}

func waitNetEvent(event *netEvent, event_lst chan netEvent, wait_time_sec int) bool {
	cond_lst := make(chan int)
	go func() {
		if wait_time_sec != 0 {
			duration := time.Duration(wait_time_sec) * time.Second
			time.Sleep(time.Duration(duration))
		}
		cond_lst <- 1
	}()

	select {
	case *event = <-event_lst:
		return true
	case <-cond_lst:
		return false
	}
}
