package conn_pool

import (
	"sdk/logger"
	"net"
	"time"
)

type ParseFunc func(recv_buf []byte) ([]byte, int)

type ConnFunc struct {
	Af_conn_func func(net_addr string, has_connected bool) (*[]byte, ParseFunc, interface{})
	Packet_func  func(connect_id int64, net_addr string, packet_buf []byte, contxt interface{})
	Af_send_err  func(net_addr string, packet_buf []byte)
}

type cConn struct {
	conn            net.Conn
	remote_addr     string
	max_packet_size int
	send_chann      chan []byte
	close_chann     chan int
	af_conn_func    func(net_addr string, has_connected bool) (*[]byte, ParseFunc, interface{})
	parse_func      ParseFunc
	packet_func     func(connect_id int64, net_addr string, packet_buf []byte, contxt interface{})
	af_send_err     func(net_addr string, packet_buf []byte)
	call_back_param interface{}
	pool            *cConnPool
	connect_id      int64
}

func (this *cConn) Send(buf []byte) bool {
	time_chann := make(chan int, 1)
	go func() {
		time.Sleep(10 * time.Second)
		time_chann <- 1
	}()

	select {
	case this.send_chann <- buf:
		return true
	case <-time_chann:
		return false
		//case
	}

}

func (this *cConn) Close() {
	this.close_chann <- 1
}

func (this *cConn) RemoteAddr() string {
	return this.remote_addr
}

func (this *cConn) sendRoutine(buf *[]byte) {
	log_obj := logger.Instance()

	defer func() {
		this.conn.Close() // 关闭连接
		event := hubEvent{
			event_type: CLOSE_CONNS,
			params:     this,
		}
		this.pool.sendEvent(&event, 10)
	}()

	// 首先发送认证数据包，如果存在的话
	if buf != nil {
		this.Send(*buf)
	}

	for {
		select {
		case send_data := <-this.send_chann:
			_, err := this.conn.Write(send_data)
			if err != nil {
				log_obj.LogSysWarn("Send Failed!ErrString=%s", err.Error())
				return
			}
			break
		case <-this.close_chann:
			return
		}
	}
}

func (this *cConn) clearSendChann() {
	// 这里使用对send_chann使用range遍历会阻塞，需要调用close函数进行关闭
	close(this.send_chann)
	for v := range this.send_chann {
		go this.af_send_err(this.remote_addr, v)
	}
}

func (this *cConn) recvRoutine() {
	defer func() {
		this.conn.Close()

		// 同时发送“退出"信号给发送协程
		this.Close()
	}()

	log_obj := logger.Instance()
	packet_buf := make([]byte, this.max_packet_size)
	offset := 0
	parse_offset := 0

	for {
		size, err := this.conn.Read(packet_buf[offset:])
		if err != nil {
			log_obj.LogSysWarn("Read Failed!ErrString=%s", err.Error())
			return
		}

		offset += size
		for {
			buf, packet_size := this.parse_func(packet_buf[parse_offset:offset])
			if packet_size == 0 { // 解析不完整
				break
			}

			this.packet_func(this.connect_id, this.remote_addr, buf, this.call_back_param)
			parse_offset += packet_size
		}

		if offset >= this.max_packet_size {
			copy(packet_buf, packet_buf[parse_offset:offset])
			offset -= parse_offset
			parse_offset = 0
		}

		// 缓冲区已经满
		if offset >= this.max_packet_size {
			log_obj.LogSysError("remoteIP=%s,Packet Full!", this.remote_addr)
			return
		}
	}

}

func createConn(remote_addr string,
	max_packet_size int,
	set_func ConnFunc,
	pool *cConnPool,
	connect_id int64) *cConn {

	// 连接远程服务
	conn, err := net.DialTimeout("tcp", remote_addr, 10*time.Second)
	if err != nil {
		log_obj := logger.Instance()
		log_obj.LogSysWarn("Connect Failed!RemoteIP=%s,ErrString=%s", remote_addr, err.Error())

		// 连接失败
		_, _, _ = set_func.Af_conn_func(remote_addr, false)
		return nil
	}

	buf, func_, contxt := set_func.Af_conn_func(remote_addr, true)

	conn_client := &cConn{
		send_chann:      make(chan []byte, 1024),
		close_chann:     make(chan int, 1), // 无需等待立即返回
		remote_addr:     remote_addr,
		af_conn_func:    set_func.Af_conn_func,
		parse_func:      func_,
		af_send_err:     set_func.Af_send_err,
		conn:            conn,
		pool:            pool,
		max_packet_size: max_packet_size,
		call_back_param: contxt,
		packet_func:     set_func.Packet_func,
		connect_id:      connect_id,
	}

	go conn_client.recvRoutine()
	go conn_client.sendRoutine(buf)

	return conn_client
}
