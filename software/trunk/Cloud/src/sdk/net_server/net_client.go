package net_server

import (
	"net"
	"sdk/logger"
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
	parse_func      func(buf []byte) ([]byte, int, int)
}

func createNetClient(config *clientConfig,
	net_func INetFunc,
	conn net.Conn,
	parse_func func(buf []byte) ([]byte, int, int)) *netClient {

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

	go net_client.run()
	return net_client
}

func (this *netClient) run() {
	var event netEvent

	recv_data_offset := 0
	recv_data_len := 0
	send_data_offset := 0
	send_data_len := 0
	last_elasped_time := time.Now().Unix()

	packet_buf := make([]byte, this.max_packet_size)
	accept_conn := this.conn
	defer accept_conn.Close()

	for {
		for {
			// 设置读写粒度为1s
			dead_time := time.Now()
			dead_time.Add(1)
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

						this.net_func.OnNetErr(this, this.contxt)
						return
					}
					break
				} else {
					this.logger.LogSysWarn("Connection Closed!ErrString=%s", err.Error())
					this.net_func.OnNetErr(this, this.contxt)
					return
				}
			}

			last_elasped_time = time.Now().Unix()
			recv_data_len += bytes_size

			// 解析包
			for {
				end_data_offset := recv_data_offset
				end_data_offset += recv_data_len

				buf, parsed_len, hash_key := this.net_func.OnParsing(this,
					packet_buf[recv_data_offset:end_data_offset],
					this.contxt)

				if parsed_len < 0 { // 返回参数如果小于0， 断开连接
					return
				} else if parsed_len == 0 { // 解析完毕，退出循环
					break
				}

				recv_data_offset += int(parsed_len)
				recv_data_len -= int(parsed_len)
			}
			if recv_data_len >= len(packet_buf) { // 缓冲区已经满
				this.logger.LogSysError("Packet Size Out Of Range!")
				this.net_func.OnNetErr(this, this.contxt)
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
				if !waitNetEvent(&event, this.io_chan, 0) { // 队列为空
					continue
				}
				if event.event_type == NET_CLOSE {
					this.logger.LogSysWarn("Connection Closed by Client!")
					this.net_func.OnNetErr(this, this.contxt)
					return
				}
			}

			// 更新时间戳
			last_elasped_time = time.Now().Unix()

			// 写超时1s
			dead_time := time.Now()
			dead_time.Add(1)
			accept_conn.SetDeadline(dead_time)
			bytes_size, err = accept_conn.Write(event.data_buf[send_data_offset:])
			send_data_len -= bytes_size
			if err != nil {
				opt_err := err.(*net.OpError)
				if opt_err.Timeout() {
					this.logger.LogSysError("Send Buf TimeOut!")
					break // 跳出写数据
				}

				this.logger.LogSysWarn("Send Failed!ErrString=%s", err.Error())
				this.net_func.OnNetErr(this, this.contxt)
				return
			}
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
