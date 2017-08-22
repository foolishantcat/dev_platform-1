package stub

import (
	"container/list"
	"sync"
)

type StubPool struct {
	idel_stub_list *list.List
	max_stub_nums  int
	min_stub_nums  int
	has_stub_nums  int
	locker         *sync.Mutex
	ip_address     string
	time_out_ms    int
}

func CreateStubPool(ip_address string, max_stub_nums, min_stub_nums int, time_out_ms int) *StubPool {
	stub_pool := &StubPool{
		idel_stub_list: list.New(),
		max_stub_nums:  max_stub_nums,
		min_stub_nums:  min_stub_nums,
		has_stub_nums:  0,
		locker:         &sync.Mutex{},
		ip_address:     ip_address,
		time_out_ms:    time_out_ms,
	}

	for i := 0; i < stub_pool.min_stub_nums; i++ {
		stub := CreateConnsStub(ip_address, time_out_ms, 1024, stub_pool)
		stub_pool.idel_stub_list.PushBack(stub)
	}
	stub_pool.has_stub_nums = stub_pool.min_stub_nums

	return stub_pool
}

func (this *StubPool) Get() *ConnsStub {

	defer this.locker.Unlock()
	this.locker.Lock()
	if this.idel_stub_list.Len() <= 0 { // 已经使用完毕

		for i := this.has_stub_nums; i < this.max_stub_nums; i++ {
			stub := CreateConnsStub(this.ip_address, this.time_out_ms, 1024, this)
			this.idel_stub_list.PushBack(stub)
		}
	}

	// 再次尝试
	if this.idel_stub_list.Len() <= 0 {
		return nil
	}

	element := this.idel_stub_list.Front()
	this.idel_stub_list.Remove(element)
	stub := element.Value.(*ConnsStub)
	this.has_stub_nums++

	return stub
}

func (this *StubPool) Retrieve(stub *ConnsStub) {
	defer this.locker.Unlock()

	this.locker.Lock()
	this.idel_stub_list.PushBack(stub)
	this.has_stub_nums--
}

func (this *StubPool) Close() {

}
