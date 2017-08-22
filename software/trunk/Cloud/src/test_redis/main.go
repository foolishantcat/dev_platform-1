package main

import (
	"fmt"
	"time"

	"sdk/serialize"

	"github.com/garyburd/redigo/redis"
)

type Person struct {
	age  int
	name string
}

func (this *Person) Serialize(ar *serialize.Archive) {
	ar.PushInt32(int32(this.age))
	ar.PushString(this.name)
}
func (this *Person) UnSerialize(ar *serialize.Archive) error {
	value, err := ar.PopInt32()
	if err != nil {
		return err
	}

	var data string
	this.age = int(value)
	data, err = ar.PopString()
	if err != nil {
		return err
	}
	this.name = data
	return nil
}

func main() {

	// 创建redis实例
	opt_durcation := 3 * time.Second
	redis_opt, err := redis.DialTimeout("tcp", "127.0.0.1:12306", opt_durcation, opt_durcation, opt_durcation)
	if err != nil {
		fmt.Printf("Redis Init Failed!ErrString=%s", err.Error())
		return
	}

	//
	var person Person

	person.age = 12
	person.name = "123"

	buf := serialize.SerializeData(&person)

	_, err = redis_opt.Do("SET", "jimmy", buf)
	if err != nil {
		fmt.Printf("Set Failed!ErrString=%s", err)
		return
	}

	reply, err := redis_opt.Do("GET", "jimmy")
	if err != nil {
		fmt.Printf("Get Failed!ErrString=%s", err.Error())
		return
	}

	if reply == nil {
		fmt.Printf("Get Nil")
		return
	}

	buf, err = redis.Bytes(reply, err)
	if err != nil {
		fmt.Printf("Convert Failed!ErrString=%s", err.Error())
		return
	}

	var person1 Person
	serialize.UnSerializeData(buf, &person1)

	fmt.Printf("Name=%s,Age=%d", person1.name, person1.age)
}
