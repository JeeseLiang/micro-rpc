package main

import (
	"encoding/json"
	"fmt"
	"log"
	"microrpc"
	"microrpc/codec"
	"net"
	"time"
)

func startServer(addr chan string) {
	// pick a free port
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	microrpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	// in fact, following code is like a simple microrpc client
	conn, _ := net.Dial("tcp", <-addr)
	defer func() { _ = conn.Close() }()

	time.Sleep(time.Second)
	// send options
	_ = json.NewEncoder(conn).Encode(microrpc.DefaultOption)
	cc := codec.NewGobCodec(conn)
	// send request & receive response
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			ID:            uint64(i),
		}
		_ = cc.Write(h, fmt.Sprintf("microrpc req %d", h.ID))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
