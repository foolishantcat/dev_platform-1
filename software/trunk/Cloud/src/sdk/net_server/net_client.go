package net_server

import (
	"net"
	"sdk/logger"
	"sync/atomic"
	"time"
)

type netClient struct {
	user_id         int64
	net_server      *netServer
	alram_time      int
	max_packet_size int
	net_func        INetFunc
	io_chan         chan []byte
	close_chan      chan int
	logger          logger.ILogger
	conn            net.Conn
	parse_func      func(buf []byte) ([]byte, int)
	params          interface{}
}

var user_id int64 = 0

func createNetClient(
	net_server *netServer,
	config *clientConfig,
	net_func INetFunc,
	conn net.Conn,
	parse_func func(buf []byte) ([]byte, int), params interface{}) bool {

	io_chan := make(chan []byte, config.max_out_packet_nums)
	close_chan := make(chan int)
	logger := logger.Instance()
	max_packet_size := config.max_packet_size
	alram_time := config.alram_time

	user_id = atomic.AddInt64(&user_id, 1)

	net_client := &netClient{
		user_id,
		net_server,
		alram_time,
		max_packet_size,
		net_func,
		io_chan,
		close_chan,
		logger,
		conn,
		parse_func,
		params,
	}

	if !net_server.addConn(user_id, net_client) {
		return false
	}

	go net_client.run()
	return true
}

func (this *netClient) run() {
	var send_buf []byte

	recv_data_offset := 0
	recv_data_len := 0
	send_data_offset := 0
	send_data_len := 0
	last_elasped_time := time.Now().Unix()
	net_server := this.net_server

	packet_buf := make([]byte, this.max_packet_size)
	accept_conn := this.conn
	defer func() {

		net_server.maps_lock.Lock()
		delete(net_server.mapclient, this.user_id)
		net_server.maps_lock.Unlock()

		accept_conn.Close()
	}()

	for {
		for {
			// 判断是否收到断开连接标志
			if this.waitNetClientClosed(0) {
				this.logger.LogSysInfo("NetClient Closed!PeerIP=%s", accept_conn.RemoteAddr().String())
				return
			}

			// 设置读写粒度为10ms
			dead_time := time.Now()
			dead_time.Add(time.Millisecond * 10)
			accept_conn.SetDeadline(dead_time)

			offset := recv_data_offset
			offset += recv_data_len

			// 开始读数据
			bytes_size, err := accept_conn.Read(packet_buf[offset:])
			if err != nil {
				opt_err := err.(*net.OpError)
				if opt_err.Timeout() { // 超时
					if time.Now().Unix()-last_elasped_time >= int64(this.alram_time) {
						this.logger.LogSysWarn("Connection TimeOut!")

						this.net_func.OnNetErr(this.user_id, this.params)
						return
					}
					break
				} else {
					this.logger.LogSysWarn("Connection Closed!ErrString=%s", err.Error())
					this.net_func.OnNetErr(this.user_id, this.params)
					return
				}
			}

			last_elasped_time = time.Now().Unix()
			recv_data_len += bytes_size

			// 解析包
			for {
				end_data_offset := recv_data_offset
				end_data_offset += recv_data_len

				buf, parsed_len := this.parse_func(packet_buf[recv_data_offset:end_data_offset])

				if parsed_len < 0 { // 返回参数如果小于0， 断开连接
					return
				} else if parsed_len == 0 { // 解析完毕，退出循环
					break
				}
				this.net_func.OnHandler(this.user_id, buf, this.params)

				recv_data_offset += int(parsed_len)
				recv_data_len -= int(parsed_len)
			}
			if recv_data_len >= len(packet_buf) { // 缓冲区已经满
				this.logger.LogSysError("Packet Size Out Of Range!")
				this.net_func.OnNetErr(this.user_id, this.params)
				return
			}
			if (recv_data_offset + recv_data_len) >= len(packet_buf) { // 重置offset
				for i := 0; i < recv_data_len; i++ {
					packet_buf[i] = packet_buf[recv_data_offset+i]
				}
				recv_data_offset = 0
			}
		}

		// 发送数据
		for {
			if send_data_len == 0 {
				// 重新读取发送队列通道
				if !waitNetEvent(&send_buf, this.io_chan, 0) { // 队列为空
					break
				}
				send_data_offset = 0
			}

			// 更新时间戳
			last_elasped_time = time.Now().Unix()

			// 写超时10ms
			dead_time := time.Now()
			dead_time.Add(time.Millisecond * 10)
			accept_conn.SetDeadline(dead_time)
			bytes_size, err := accept_conn.Write(send_buf[send_data_offset:])
			send_data_len -= bytes_size
			send_data_offset += bytes_size
			if err != nil {
				opt_err := err.(*net.OpError)
				if opt_err.Timeout() {
					this.logger.LogSysError("Send Buf TimeOut!")
					break // 跳出写数据
				}

				this.logger.LogSysWarn("Send Failed!ErrString=%s", err.Error())
				this.net_func.OnNetErr(this.user_id, this.params)
				return
			}
		}
	}

}

func (this *netClient) Send(buff []byte) bool {
	time_out_chan := make(chan int)

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

func waitNetEvent(send_buf *[]byte, event_lst chan []byte, wait_time_sec int) bool {
	cond_lst := make(chan int)
	go func() {
		if wait_time_sec != 0 {
			duration := time.Duration(wait_time_sec) * time.Second
			time.Sleep(time.Duration(duration))
		}
		cond_lst <- 1
	}()

	select {
	case *send_buf = <-event_lst:
		return true
	case <-cond_lst:
		return false
	}
}

func (this *netClient) waitNetClientClosed(wait_time_sec int) bool {
	cond_lst := make(chan int)
	go func() {
		if wait_time_sec != 0 {
			duration := time.Duration(wait_time_sec) * time.Second
			time.Sleep(time.Duration(duration))
		}
		cond_lst <- 1
	}()

	select {
	case <-this.close_chan:
		return true
	case <-cond_lst:
		return false
	}
}
