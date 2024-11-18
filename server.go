package microrpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"microrpc/codec"
	"net"
	"reflect"
	"sync"
)

const FLAG_NUMBER = 0x3bf1bd

type Option struct { // 选项信息
	MarkNumber int        // 标记号
	CodecType  codec.Type // 编码方式
}

type request struct { // 请求信息
	h          *codec.Header
	arg, reply reflect.Value
}

var DefaultOption = &Option{
	MarkNumber: FLAG_NUMBER,
	CodecType:  codec.GOB, // 默认使用GOB编码
}

/*
micro-rpc采用的协议格式

| Option{MarkNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
*/

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

var (
	DefaultServer  = NewServer()
	invalidRequest = struct{}{}
)

func (server *Server) serveCodec(f codec.Codec) {
	// 加锁确保发送完整的消息
	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	// 读取，处理，发送
	for {
		req, err := server.readRequest(f)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(f, req.h, invalidRequest, mutex)
			continue
		}
		wg.Add(1)
		go server.handleRequest(f, req, mutex, wg)
	}
	wg.Wait()
	_ = f.Close()
}

func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer conn.Close()

	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Printf("rpc server : decode option error:%v\n", err)
		return
	}

	if opt.MarkNumber != FLAG_NUMBER {
		log.Printf("rpc server : invalid mark number error:%d\n", opt.MarkNumber)
		return
	}

	f := codec.CodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server : invalid codec type error:%v\n", opt.CodecType)
		return
	}

	server.serveCodec(f(conn))
}

func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Fatalf("rpc server : accept error:%v\n", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

func (server *Server) readRequestHeader(f codec.Codec) (*codec.Header, error) {
	h := codec.Header{}
	if err := f.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Printf("rpc server : read header error:%v\n", err)
			return nil, err
		}
	}
	return &h, nil
}

func (server *Server) readRequest(f codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(f)
	if err != nil {
		return nil, err
	}
	// 先暂时认为他就是个字符串，后续完善
	req := &request{h: h}
	req.arg = reflect.New(reflect.TypeOf(""))
	if err = f.ReadBody(req.arg.Interface()); err != nil {
		log.Println("rpc server: read arg err:", err)
	}
	return req, nil
}

func (server *Server) sendResponse(f codec.Codec, h *codec.Header, body interface{}, mutex *sync.Mutex) {
	mutex.Lock()
	defer mutex.Unlock()
	if err := f.Write(h, body); err != nil {
		log.Printf("rpc server : write response error:%v\n", err)
	}
}

func (server *Server) handleRequest(f codec.Codec, req *request, mutex *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println(req.h, req.arg.Elem())
	req.reply = reflect.ValueOf(fmt.Sprintf("geerpc resp %d", req.h.ID))
	server.sendResponse(f, req.h, req.reply.Interface(), mutex)
}

func Accrpt(lis net.Listener) { DefaultServer.Accept(lis) }
