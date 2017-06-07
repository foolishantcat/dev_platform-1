package main

import (
	"reflect"
	"unsafe"
)

type Archive struct {
	serial_buf   []byte
	read_offset  int
	write_offset int
}

type arcError struct {
	err_string string
}

func (err arcError) Error() string {
	return err.err_string
}

func (err *arcError) setErr(err_string string) {
	err.err_string = err_string
}

func UnSerializeData(data []byte, obj SerialObject) error {
	arc := &Archive{
		read_offset:  0,
		write_offset: len(data),
		serial_buf:   data,
	}

	return obj.UnSerialize(arc)
}

func SerializeData(obj SerialObject) []byte {
	arc := &Archive{
		read_offset:  0,
		write_offset: 0,
		serial_buf:   make([]byte, 0),
	}

	obj.Serialize(arc)

	return arc.GetBytes()
}

func (this *Archive) PushString(value string) {
	push_buf := []byte(value)

	// 先压入长度
	var chars_len int32

	chars_len = int32(len(push_buf))
	this.PushInt32(chars_len)

	// 先压入数据
	this.PushArray(push_buf)
}

func (this *Archive) PushArray(array []byte) {
	this.serial_buf = append(this.serial_buf[:this.write_offset], array...)
	this.write_offset += len(array)
}

func (this *Archive) PushInt32(value int32) {
	var slice reflect.SliceHeader

	slice.Data = (uintptr)(unsafe.Pointer(&value))
	slice.Len = int(unsafe.Sizeof(value))
	slice.Cap = int(unsafe.Sizeof(value))

	push_buf := (*[]byte)(unsafe.Pointer(&slice))
	this.PushArray(*push_buf)
}

func (this *Archive) PushInt64(value int64) {
	var slice reflect.SliceHeader

	slice.Data = (uintptr)(unsafe.Pointer(&value))
	slice.Len = int(unsafe.Sizeof(value))
	slice.Cap = int(unsafe.Sizeof(value))

	push_buf := (*[]byte)(unsafe.Pointer(&slice))
	this.PushArray(*push_buf)
}

func (this *Archive) PushSerialObj(object SerialObject) {
	object.Serialize(this)
}

func (this *Archive) PopString() (string, error) {
	var err error
	var len int32
	var buf []byte

	err = nil
	offset := this.read_offset
	defer func() {
		if err != nil {
			// 序列化的数据还原
			this.read_offset = offset

		}
	}()

	// 先弹出长度
	len, err = this.PopInt32()
	if err != nil {
		return "", err
	}

	buf, err = this.PopArray(int(len))
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (this *Archive) PopInt32() (int32, error) {
	len := (int)(unsafe.Sizeof(int32(0)))
	pop_buf, err := this.PopArray(len)
	if err != nil {
		return 0, err
	}

	value := (*int32)(unsafe.Pointer(&pop_buf[0]))
	return *value, nil
}

func (this *Archive) PopInt64() (int64, error) {
	len := (int)(unsafe.Sizeof(int64(0)))
	pop_buf, err := this.PopArray(len)
	if err != nil {
		return 0, err
	}

	value := (*int64)(unsafe.Pointer(&pop_buf[0]))
	return *value, nil
}

func (this *Archive) PopSerialObj(object SerialObject) error {
	return object.UnSerialize(this)
}

func (this *Archive) PopArray(array_size int) ([]byte, error) {
	var err arcError

	if this.write_offset-this.read_offset < array_size {
		err.setErr("Not Enough Buf!")
		return nil, &err
	}

	buf := this.serial_buf[this.read_offset : this.read_offset+array_size]
	this.read_offset += array_size

	return buf, nil
}

func (this Archive) GetBytes() []byte {
	return this.serial_buf[0:this.write_offset]
}

type SerialObject interface {
	Serialize(ar *Archive)
	UnSerialize(ar *Archive) error
}
