package net_conns

type IConnFunc interface {
	func OnConnParsing(procol_name string, server_ip, port string, buff []byte, 
						contxt interface{})int32
	func OnConnSendErr(server_ip, port string, contxt interface{})
}

type IConnsPool interface{
	func AddConn(protol_name string, server_ip, port string, config Config)
	func RmConn(protol_name string, server_ip, port string)
	func Start() bool
	func Send(server_ip, port string, buff []byte, contxt interface{}) bool
}

type connParams{
	
}

type connPool struct{
	map<string, 
}


