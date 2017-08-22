package ipc_comm

import (
	"sdk/logger"
	"unsafe"
)

func ParseData(buf []byte) ([]byte, int) {

	log_obj := logger.Instance()

	// 先收头部
	if len(buf) >= int(unsafe.Sizeof(int16(0))) {
		var packet_len int

		packet_len = (int)(*(*int16)(unsafe.Pointer(&buf[0])))
		packet_len += int(unsafe.Sizeof(int16(0)))
		log_obj.LogAppDebug("Packet Recv!Len=%d", packet_len)
		if len(buf) >= packet_len { // 加上packet_len
			packet_buf := make([]byte, packet_len)
			copy(packet_buf, buf)

			return packet_buf, packet_len
		}
	}

	return nil, 0
}
