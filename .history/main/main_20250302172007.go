package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	srpc "github.com/dszqbsm/Srpc"
	"github.com/dszqbsm/Srpc/codec"
)

func startServer(addr chan string) {
	// 监听端口
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	srpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)
	conn, _ := net.Dial("tcp", <-addr)
	defer func() { _ = conn.Close() }()

	time.Sleep(time.Second)
	// 发送选项
	_ = json.NewEncoder(conn).Encode(srpc.DefaultOption)
	cc := codec.NewGobCodec(conn)

	// 发送请求
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		_ = cc.Write(h, fmt.Sprintf("Srpc req %d", h.Seq))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
