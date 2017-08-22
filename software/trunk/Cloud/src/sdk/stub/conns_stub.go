package stub

import (
	//"sync"
	//"ipc_server/ipc_comm"
	"net"
	"protol"
	"sdk/logger"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
)

const (
	NOT_ENGOUGH_ERROR = 1
	UNPACKET_ERROR    = 2
)

// 自定义的Error
type stubError struct {
	err_code int
}

func (this stubError) Error() string {
	if this.err_code == NOT_ENGOUGH_ERROR {
		return "Not Engough Packet Buffer!"
	} else if this.err_code == UNPACKET_ERROR {
		return "Unpacket Failed!"
	} else {
		return "Unkown Error!"
	}

}

type ConnsStub struct {
	ip_address            string
	time_out_ms           int
	packet_size           int
	recv_buf              []byte
	need_recv_packet_nums int
	conn                  net.Conn
	offset                int
	create_pool           *StubPool
}

type PacketData struct {
	request  []byte
	msg_name string
}

func CreateConnsStub(ip_address string, time_out_ms int, packet_size int, pool *StubPool) *ConnsStub {

	// 真正的时候调用,采取连接
	stub := &ConnsStub{
		ip_address:  ip_address,
		time_out_ms: time_out_ms,
		recv_buf:    make([]byte, packet_size),
		conn:        nil,
		need_recv_packet_nums: 0,
		offset:                0,
		create_pool:           pool,
	}

	return stub
}

func (this *ConnsStub) SetTimeOutMs(time_out_ms int) {
	this.time_out_ms = time_out_ms
}

func (this *ConnsStub) invokeReq(request []byte, msg_name string, time_out_ms int) error {

	data := protol.Pack(request, msg_name)

	err := this.tryConnect()
	if err != nil {
		logger.Instance().LogAccError("Try Connect Failed !Host=%s,ErrString=%s", this.ip_address, err.Error())
		return err
	}

	defer func() {
		if err != nil {
			this.Close()
		}
	}()

	// 打包数据
	this.conn.SetDeadline(time.Now().Add(time.Duration(time_out_ms) * time.Millisecond))
	_, err = this.conn.Write(data)

	if err != nil {
		logger.Instance().LogAccWarn("Request Failed,Try Connect Again!Host=%s,ErrString=%s", this.ip_address, err.Error())

		// 超时，直接返回错误
		opt_err := err.(*net.OpError)
		if opt_err.Timeout() {
			return err
		}

		// 重新连接
		this.Close()

		if err = this.tryConnect(); err != nil {
			// 连接失败
			logger.Instance().LogAccError("Try Connect Failed Again!Host=%s,ErrString=%s", this.ip_address, err.Error())
			return err
		}

		this.conn.SetDeadline(time.Now().Add(time.Duration(time_out_ms) * time.Millisecond))
		_, err = this.conn.Write(data) // 再次发送数据
		if err != nil {
			logger.Instance().LogAccError("Request Failed Again!Host=%s,ErrString=%s", this.ip_address, err.Error())
			return err
		}
	}

	return nil
}

func (this *ConnsStub) waitRes(time_out_ms int) ([]byte, error) {

	var buf [1024]byte
	for {
		this.conn.SetDeadline(time.Now().Add(time.Duration(time_out_ms) * time.Millisecond))
		size, err := this.conn.Read(buf[:])
		if err != nil {
			logger.Instance().LogAccError("waitRes Failed!Host=%s,ErrString=%s", this.ip_address, err.Error())
			return nil, err
		}

		if (this.offset + size) > len(this.recv_buf) { // 缓冲区已经满
			logger.Instance().LogAccWarn("watit Res Failed!Host=%s,Recv-Buf Full!", this.ip_address)
			return nil, &stubError{NOT_ENGOUGH_ERROR}
		}

		copy(this.recv_buf[this.offset:], buf[:size])
		this.offset += size

		// 判断是否已经接受完头部
		if this.offset >= int(unsafe.Sizeof(int16(0))) {

			var packet_len int
			packet_len = (int)(*(*int16)(unsafe.Pointer(&this.recv_buf[0])))
			packet_len += int(unsafe.Sizeof(int16(0)))
			if this.offset >= packet_len { // 加上packet_len
				packet_buf := make([]byte, packet_len)
				copy(packet_buf, this.recv_buf[:packet_len])

				// 移动到缓冲区头部
				copy(this.recv_buf, this.recv_buf[packet_len:this.offset])
				this.offset = 0

				return packet_buf, nil
			}
		}
	}
}

func (this *ConnsStub) Invoke(request []byte, msg_name string) ([]byte, string, error) {

	var data []byte

	err := this.invokeReq(request, msg_name, this.time_out_ms)
	if err != nil {
		logger.Instance().LogAccError("inovkeReq Failed!Host=%s,ErrString=%s", this.ip_address, err.Error())

		return nil, "", err
	}

	data, err = this.waitRes(this.time_out_ms)
	if err != nil {
		logger.Instance().LogAccError("waitRes Failed!Host=%s,ErrString=%s", this.ip_address, err.Error())

		return nil, "", err
	}

	// 在尝试一次，有可能断开连接
	var res_buf []byte
	var ok bool
	res_buf, msg_name, ok = protol.Unpack(data)
	if !ok {
		logger.Instance().LogAccError("Unapcket Failed!Host=%s,ErrString=%s", this.ip_address)
		return nil, "", &stubError{UNPACKET_ERROR}
	}

	return res_buf, msg_name, nil
}

func (this *ConnsStub) Close() {
	if this.conn != nil {
		this.conn.Close()
		this.conn = nil

		logger.Instance().LogAccInfo("Close Connect!Host=%s", this.ip_address)
	}
}

func (this *ConnsStub) tryConnect() error {

	// 先进行写数据
	if this.conn == nil {

		// 还木有连接上服务,先连接服务
		conn, err := net.DialTimeout("tcp", this.ip_address, time.Duration(this.time_out_ms)*time.Millisecond)
		if err != nil {
			logger.Instance().LogAccError("Connect Failed!Host=%s,ErrString=%s", this.ip_address, err)
			return err
		}

		this.conn = conn
	}

	return nil
}

func (this *ConnsStub) InvokePb(req proto.Message, msg_name string, res proto.Message) (string, error) {
	buf, err := proto.Marshal(req)
	if err != nil {
		return "", err
	}

	buf, msg_name, err = this.Invoke(buf, msg_name)

	//pb_message proto.Message
	err = proto.Unmarshal(buf, res)
	if err != nil {
		return "", err
	}

	return msg_name, nil
}
