package net_server

import (
	"net"
	"sdk/logger"
	"sync"
	"sync/atomic"
)

type INetFunc interface {
	OnNetAccpet(peer_ip string) (bool, func(buf []byte) ([]byte, int))
	OnHandler(user_id int64, packet_buf []byte, params interface{})
	OnNetErr(user_id int64, params interface{})
}

type INetServer interface {
	AddParams(param interface{})
	Start(config *Config) bool
	Send(user_id int64, packet_buf []byte)
	Close(user_id int64)
	Stop()
}

type clientConfig struct {
	max_packet_size     int
	max_out_packet_nums int
	alram_time          int
}
type netServer struct {
	local_ip    string
	port        string
	client_conf clientConfig
	params      interface{}
	net_func    INetFunc
	listen_chan chan netEvent // 用来停止监听协程的
	logger      logger.ILogger

	max_accpet_nums int32
	has_accpet_nums int32
	maps_lock       *sync.RWMutex // 这里采用读写锁,毕竟有可能长连接
	mapclient       map[int64]*netClient
	atomic_id       int64
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
	net_server.params = nil
	net_server.maps_lock = new(sync.RWMutex)
	net_server.mapclient = make(map[int64]*netClient)
	net_server.atomic_id = 0

	return net_server
}

func (this *netServer) AddParams(params interface{}) {
	this.params = params
}

func (this *netServer) Start(config *Config) bool {
	this.local_ip = config.bind_conf.ip_address
	this.port = config.bind_conf.port
	this.client_conf.alram_time = config.alram_time
	this.client_conf.max_out_packet_nums = config.max_out_packet_nums
	this.client_conf.max_packet_size = config.max_packet_size
	this.max_accpet_nums = int32(config.max_accpet_nums)
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

			ok, parser := this.net_func.OnNetAccpet(conn.RemoteAddr().String())
			if ok {
				if createNetClient(this,
					&this.client_conf,
					this.net_func,
					conn,
					parser,
					this.params) {

					this.logger.LogSysInfo("CreateNetClient Failed!PeerIP=%s", conn.RemoteAddr().String())
					conn.Close()
				}

			} else {
				this.logger.LogSysInfo("Remote Client Closed!PeerIP=%s", conn.RemoteAddr().String())
				conn.Close()
			}
		}
	}
}

func (this *netServer) Stop() {

}

func (this *netServer) Send(user_id int64, packet_buf []byte) {
	var ok bool = false
	var client *netClient = nil

	func() {
		defer this.maps_lock.RUnlock()
		this.maps_lock.RLock()
		client, ok = this.mapclient[user_id]
	}()

	if ok {
		client.Send(packet_buf)
	}
}

func (this *netServer) Close(user_id int64) {
	var ok bool = false
	var client *netClient = nil

	func() {
		defer this.maps_lock.RUnlock()
		this.maps_lock.RLock()
		client, ok = this.mapclient[user_id]
	}()

	if ok {
		client.Close()
	}
}

func (this *netServer) addConn(user_id int64, client *netClient) bool {
	accpet_nums := atomic.AddInt32(&this.has_accpet_nums, 1)
	if accpet_nums >= this.max_accpet_nums {
		this.logger.LogSysInfo("Accept Nums OutOf Range!NowNums=%d,MaxNums=%d",
			accpet_nums,
			this.max_accpet_nums)
		atomic.AddInt32(&this.has_accpet_nums, -1)
		return false
	}
	defer this.maps_lock.Unlock()
	this.maps_lock.Lock()
	this.mapclient[user_id] = client

	return true
}

func (this *netServer) rmConn(user_id int64) {
	defer this.maps_lock.Unlock()
	this.maps_lock.Lock()
	delete(this.mapclient, user_id)
}
