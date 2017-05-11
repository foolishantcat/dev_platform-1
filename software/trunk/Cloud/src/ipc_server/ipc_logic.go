package main

import (
	"sdk/net_server"
	"sdk/logger"
	"github.com/garyburd/redigo/redis"
)

type INetFunc interface {
    OnNetAccpet(peer_ip string) (interface{}, bool, func(buf []byte) ([]byte, int))
    OnNetRecv(client INetClient, packet_buf []byte, contxt interface{})
    OnNetErr(client INetClient, contxt interface{})
}

type ServerLogic struct{
	redis_conn redis.Conn
}

func (this *ServerLogic)OnNetAccpet(peer_ip string) (interface{}, bool, func(buf []byte) ([]byte, int))
{
	
}

func (this *ServerLogic)OnNetRecv(client INetClient, packet_buf []byte, contxt interface{}){
	
}

func (this *ServerLogic)OnNetErr(client INetClient, contxt interface{}){
	
}

func (this *ServerLogic)Initialize(){
	
}
