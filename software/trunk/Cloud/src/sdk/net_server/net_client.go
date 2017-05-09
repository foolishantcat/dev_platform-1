package net_server

import (
	"sdk/logger"
	"net"
	"time"
)

type INetClient interface {
	Send(send_buf []byte) bool
	Close()
	GetPeerAddr() (ip_address string)
}

type netClient struct {
	alram_time      int
	max_packet_size int
	net_func        INetFunc
	io_chan         chan []byte
	close_chan      chan int
	logger          logger.ILogger
	conn            net.Conn
	parse_func      func(buf []byte) ([]byte, int)
	contxt          interface{}
}

func createNetClient(config *clientConfig,
	net_func INetFunc,
	conn net.Conn,
	contxt interface{},
	parse_func func(buf []byte) ([]byte, int)) *netClient {

	io_chan := make(chan []byte, config.max_out_packet_nums)
	close_chan := make(chan int)
	logger := logger.Instance()
	max_packet_size := config.max_packet_size
	alram_time := config.alram_time

	net_client := &netClient{
		alram_time,
		max_packet_size,
		net_func,
		io_chan,
		close_chan,
		logger,
		conn,
		parse_func,
		contxt,
	}

	go net_client.sendRoutine()
	go net_client.recvRountine()
	return net_client
}

func (this *netClient) sendRoutine() {

	// 发送数据
	accept_conn := this.conn
	defer accept_conn.Close()
	for {
		select {
		case data_buf := <-this.io_chan:
			_, err := accept_conn.Write(data_buf)
			if err != nil {
				this.logger.LogSysWarn("Send Failed!ErrString=%s,PeerIP=%s", err.Error(), accept_conn.RemoteAddr())
				return
			}
			break
		case <-this.close_chan:
			this.logger.LogSysWarn("Close Client!PeerIP=%s", accept_conn.RemoteAddr())
			return
		}
	}
}

func (this *netClient) recvRountine() {
	parse_offset := 0
	offset := 0

	packet_buf := make([]byte, this.max_packet_size)
	accept_conn := this.conn
	defer func() {
		accept_conn.Close()
		this.net_func.OnNetErr(this, this.contxt)
	}()
	for {
		// 设置超时时间
		accept_conn.SetReadDeadline(time.Now().Add(time.Duration(this.alram_time) * time.Second))

		// 开始读数据
		bytes_size, err := accept_conn.Read(packet_buf[offset:])
		if err != nil {
			this.logger.LogSysWarn("PeerIP=%s,Connection Closed!ErrString=%s", accept_conn.RemoteAddr(), err.Error())
			return
		}
		offset += bytes_size

		// 解析包
		for {
			buf, size_parsed := this.parse_func(packet_buf[parse_offset:offset])
			if size_parsed == 0 { // 接受不完整，退出
				break
			}

			this.net_func.OnNetRecv(this, buf, this.contxt)
			parse_offset += size_parsed
		}

		if parse_offset >= len(packet_buf) { // 缓冲区已经满
			this.logger.LogSysError("PeerIP=%s,Packet Size Out Of Range!", accept_conn.RemoteAddr())
			this.net_func.OnNetErr(this, this.contxt)
			return
		}
		if offset >= len(packet_buf) { // 重置offset
			copy(packet_buf[0:], packet_buf[parse_offset:offset])
			offset -= parse_offset
			parse_offset = 0
		}
	}
}

func (this *netClient) Send(buff []byte) bool {
	time_out_chan := make(chan int, 1)

	// channel超时
	go func() {
		time.Sleep(10 * time.Second)
		time_out_chan <- 1
	}()

	select {
	case this.io_chan <- buff:
		return true
	case <-time_out_chan:
		return false
	}
}

func (this *netClient) Close() {
	this.close_chan <- 1
}

func (this *netClient) GetPeerAddr() string {
	return this.conn.LocalAddr().String()
}
