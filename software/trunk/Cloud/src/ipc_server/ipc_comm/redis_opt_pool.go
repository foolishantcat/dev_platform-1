package ipc_comm

import (
	"container/list"
	"sdk/logger"
	"sync"
)

type RedOptPool struct {
	idel_opt_lst  *list.List
	alloc_opt_lst *list.List
	locker        *sync.Mutex
	min_nums      int
	max_nums      int
	opt_time      int
	has_nums      int
	ip_address    string
}

func CreateRedsOptPool(ip_address string, min_nums int, max_nums int, opt_time int) *RedOptPool {
	redis_pool := &RedOptPool{
		idel_opt_lst:  list.New(),
		alloc_opt_lst: list.New(),
		locker:        &sync.Mutex{},
		min_nums:      min_nums,
		max_nums:      max_nums,
		opt_time:      opt_time,
		ip_address:    ip_address,
		has_nums:      min_nums,
	}

	var ok bool

	ok = false
	defer func() {
		if !ok {
			for i := 0; i < redis_pool.alloc_opt_lst.Len(); i++ {
				element := redis_pool.alloc_opt_lst.Front()
				redis_opt := element.Value.(*RedisOpt)
				redis_opt.free()
				redis_pool.alloc_opt_lst.Remove(element)
			}
		}

	}()

	for i := 0; i < min_nums; i++ {
		redis_opt := CreateRedisOpt(ip_address, opt_time, redis_pool)
		if redis_opt == nil {
			logger.Instance().LogAppError("Create RedisOpt Failed!Host=%s", ip_address)
			return nil
		}
		redis_pool.idel_opt_lst.PushBack(redis_opt)
		redis_pool.alloc_opt_lst.PushBack(redis_opt)
	}

	ok = true
	return redis_pool
}

func (this *RedOptPool) Get() *RedisOpt {
	this.locker.Lock()
	defer this.locker.Unlock()

	if this.idel_opt_lst.Len() <= 0 {
		this.alloc() // 重新申请连接池
	}
	if this.idel_opt_lst.Len() <= 0 {
		return nil
	}
	element := this.idel_opt_lst.Front()
	return element.Value.(*RedisOpt)

}

func (this *RedOptPool) Close() {

	// 断开所有的连接
	for i := 0; i < this.alloc_opt_lst.Len(); i++ {
		element := this.alloc_opt_lst.Front()
		redis_opt := element.Value.(*RedisOpt)
		redis_opt.free()
		this.alloc_opt_lst.Remove(element)

	}
}

func (this *RedOptPool) alloc() {

	// 放大两部
	nums := this.has_nums * 2
	if nums > this.max_nums {
		nums = this.max_nums
	}
	for ; this.has_nums < nums; this.has_nums++ {
		redis_opt := CreateRedisOpt(this.ip_address, this.opt_time, this)
		if redis_opt != nil {
			logger.Instance().LogAppError("alloc Failed!Host=%s", this.ip_address)
			return
		}

		this.alloc_opt_lst.PushBack(redis_opt)
		this.idel_opt_lst.PushBack(redis_opt)

		this.has_nums++
	}
}

func (this *RedOptPool) retrieve(redis_opt *RedisOpt) {
	this.locker.Lock()
	this.idel_opt_lst.PushBack(redis_opt)
	this.locker.Unlock()
}
