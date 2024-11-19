package microrpc

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"io"
	"log"
	"microrpc/codec"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/*
micro-rpc采用的协议格式

| Option{MarkNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
*/

const FLAG_NUMBER = 0x3bf1bd

var (
	DefaultOption = &Option{
		MarkNumber:     FLAG_NUMBER,
		CodecType:      codec.GOB,        // 默认使用GOB编码
		ConnectTimeout: time.Second * 10, // 默认连接超时时间
	}
	DefaultServer  = NewServer()
	invalidRequest = struct{}{}
)

// 选项信息
type Option struct {
	MarkNumber     int           // 标记号
	CodecType      codec.Type    // 编码方式
	ConnectTimeout time.Duration // 连接超时时间
	HandleTimeout  time.Duration // 处理超时时间
}

// 请求信息
type request struct {
	h          *codec.Header
	arg, reply reflect.Value
	mtype      *methodType
	svc        *service
}

// 服务信息
type Server struct {
	services sync.Map // 服务列表
}

func NewServer() *Server {
	return &Server{}
}

func (server *Server) Register(reveiver interface{}) error {
	s := newService(reveiver)
	if _, loaded := server.services.LoadOrStore(s.name, s); loaded {
		return fmt.Errorf("rpc server : service %s already registered", s.name)
	}
	return nil
}
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

func Register(receiver interface{}) error {
	return DefaultServer.Register(receiver)
}

func (server *Server) serveCodec(f codec.Codec, opt *Option) {
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
		go server.handleRequest(f, req, mutex, wg, opt.HandleTimeout)
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

	server.serveCodec(f(conn), &opt)
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

	req := &request{h: h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}

	req.arg = req.mtype.newArgv()
	req.reply = req.mtype.newReplyv()

	argvi := req.arg.Interface()
	// 确保是指针类型
	if req.arg.Type().Kind() != reflect.Ptr {
		argvi = req.arg.Addr().Interface()
	}
	if err := f.ReadBody(argvi); err != nil {
		log.Printf("rpc server : read body error:%v\n", err)
		return req, err
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

func (server *Server) handleRequest(f codec.Codec, req *request, mutex *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	sent := make(chan struct{})
	var once sync.Once

	go func() {
		err := req.svc.call(req.mtype, req.arg, req.reply)
		if err != nil {
			req.h.Error = err.Error()
			once.Do(func() {
				server.sendResponse(f, req.h, invalidRequest, mutex)
				sent <- struct{}{}
			})
		}
		once.Do(func() {
			server.sendResponse(f, req.h, req.reply.Interface(), mutex)
			sent <- struct{}{}
		})
	}()

	if timeout == 0 {
		<-sent
		return
	}

	select {
	case <-time.After(timeout):
		once.Do(func() {
			server.sendResponse(f, req.h, invalidRequest, mutex)
			sent <- struct{}{}
		})
	case <-sent:
		return
	}
}

func (server *Server) findService(serviceMethod string) (*service, *methodType, error) {
	args := strings.Split(serviceMethod, ".")
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("rpc server : invalid service method %s", serviceMethod)
	}
	serviceName, methodName := args[0], args[1]
	s, ok := server.services.Load(serviceName)
	if !ok {
		return nil, nil, fmt.Errorf("rpc server : service %s.%s not found", serviceName, methodName)
	}
	svc := s.(*service)
	mtype := svc.method[methodName]
	if mtype == nil {
		return nil, nil, fmt.Errorf("rpc server : method %s not found in service %s", methodName, serviceName)
	}
	return svc, mtype, nil
}

// 通过反射实现注册service

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	numCalls  uint64
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *methodType) newArgv() (argv reflect.Value) {
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return
}

func (m *methodType) newReplyv() reflect.Value {
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

type service struct {
	name     string
	typ      reflect.Type
	method   map[string]*methodType
	receiver reflect.Value
}

func newService(receiver interface{}) *service {
	s := &service{
		name:     reflect.Indirect(reflect.ValueOf(receiver)).Type().Name(),
		typ:      reflect.TypeOf(receiver),
		receiver: reflect.ValueOf(receiver),
	}
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server : service %s is invalid name\n", s.name)
	}

	s.registerMethods()

	return s
}

func isValidType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		// 根据rpc函数的定义，入参第一个参数是参数，第二个参数是返回值
		// 但是由于反射的原因，自身会作为第零个参数，类似于this和self
		// 出参只有一个error
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isValidType(argType) || !isValidType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server : register method %s.%s\n", s.name, method.Name)
	}
}

func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	retValue := m.method.Func.Call([]reflect.Value{s.receiver, argv, replyv})
	if err := retValue[0].Interface(); err != nil {
		return err.(error)
	}
	return nil
}
