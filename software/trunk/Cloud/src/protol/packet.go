package protol

import (
	"reflect"
	"unsafe"
)

func Pack(body []byte, msg_name string) []byte {
	var packet_len = 0
	var slice_header reflect.SliceHeader

	name_len := len(msg_name)
	packet_len += int(unsafe.Sizeof(int16(0))) // Sizeof(name_len)
	packet_len += name_len
	if body != nil {
		packet_len += len(body)
	}

	packet_buf := make([]byte, 0, 256)
	slice_header.Cap = int(unsafe.Sizeof(int16(0)))
	slice_header.Len = int(unsafe.Sizeof(int16(0)))
	slice_header.Data = (uintptr)(unsafe.Pointer(&packet_len))

	data_buf := (*[]byte)(unsafe.Pointer(&slice_header))
	packet_buf = append(packet_buf, *data_buf...) // Sizeof(packet_len)

	slice_header.Cap = int(unsafe.Sizeof(int16(0)))
	slice_header.Len = int(unsafe.Sizeof(int16(0)))
	slice_header.Data = (uintptr)(unsafe.Pointer(&name_len))
	data_buf = (*[]byte)(unsafe.Pointer(&slice_header))
	packet_buf = append(packet_buf, *data_buf...) // Sizeof(packet_len)

	packet_buf = append(packet_buf, ([]byte(msg_name))...)

	if body != nil {
		packet_buf = append(packet_buf, body...)
	}

	return packet_buf
}

func Unpack(buf []byte) ([]byte, string, bool) {
	var packet_len int
	var body []byte
	packet_offset := 0

	packet_len = int(*(*int16)(unsafe.Pointer(&buf[0])))

	if packet_len < packet_offset+int(unsafe.Sizeof(int16(0))) { // 小于msg_len长度
		return nil, "", false
	}

	name_len := int(*(*int16)(unsafe.Pointer(&buf[2])))
	packet_offset += int(unsafe.Sizeof(int16(0)))
	packet_data := buf[2:]

	if packet_len < packet_offset+name_len {
		return nil, "", false
	}

	message_name := string(packet_data[packet_offset : packet_offset+name_len])
	packet_offset += name_len

	if packet_len-packet_offset > 0 {
		body = make([]byte, packet_len-packet_offset)
		copy(body, packet_data[packet_offset:])
	}

	return body, message_name, true
}
