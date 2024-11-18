package microrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"microrpc/codec"
	"net"
	"sync"
)

type Call struct {
	ID            uint64
	ServiceMethod string
	Error         error
	Args          interface{}
	Reply         interface{}
	Done          chan *Call
}

type Client struct {
	// 处理发送请求信息
	sending sync.Mutex
	cc      codec.Codec
	header  codec.Header
	option  *Option
	// Client状态
	ID       uint64
	mutex    sync.Mutex
	calls    map[uint64]*Call
	closing  bool
	shutdown bool
}

var (
	Errshutdown           = errors.New("client is shutdown")
	_           io.Closer = (*Client)(nil)
)

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.CodecFuncMap[opt.CodecType]
	if f == nil {
		log.Println("rpc client: unsupported codec type")
		return nil, errors.New("unsupported codec type")
	}
	cc := f(conn)
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: encode option error:", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(cc, opt)
}

func newClientCodec(cc codec.Codec, opt *Option) (*Client, error) {
	client := &Client{
		cc:     cc,
		option: opt,
		calls:  make(map[uint64]*Call),
		ID:     1,
	}

	go client.Start()
	return client, nil
}

func (c *Client) Start() {
	// 不遇到错误的情况下，一直循环工作
	var err error
	for err == nil {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); err != nil {
			break
		}
		call := c.calls[h.ID]
		switch {
		case call == nil:
			err = c.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = c.cc.ReadBody(nil)
			call.DoneCall()
		default:
			err = c.cc.ReadBody(call.Reply)
			if err != nil {
				log.Println("rpc client: read body error:", err)
			}
			call.DoneCall()
		}
	}
	c.TerminateCall(err)
}

func (c *Client) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closing {
		return Errshutdown
	}
	c.closing = true
	return c.cc.Close()
}

// 注册一个Call，并返回其ID
func (c *Client) RegisterCall(call *Call) (uint64, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.shutdown || c.closing {
		return 0, Errshutdown
	}
	call.ID = c.ID
	c.ID++
	c.calls[call.ID] = call
	return call.ID, nil
}

// 移除一个Call，并返回其信息
func (c *Client) RemoveCall(id uint64) (*Call, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.shutdown || c.closing {
		return nil, Errshutdown
	}
	if call, ok := c.calls[id]; !ok {
		return nil, errors.New("call not found")
	} else {
		delete(c.calls, id)
		return call, nil
	}
}

// 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call。
func (c *Client) TerminateCall(e error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.sending.Lock()
	defer c.sending.Unlock()
	c.shutdown = true
	for _, call := range c.calls {
		call.Error = e
		call.DoneCall()
	}
}

func (c *Client) IsAvailable() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return !c.shutdown && !c.closing
}

func (c *Call) DoneCall() {
	c.Done <- c
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 {
		return DefaultOption, nil
	}
	if len(opts) > 1 {
		return nil, errors.New("too many options")
	}

	opt := opts[0]
	// 合并默认选项
	opt.MarkNumber = DefaultOption.MarkNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}

	return opt, nil
}

func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	// 确保连接关闭
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()

	return NewClient(conn, opt)
}

func (c *Client) send(call *Call) {
	c.sending.Lock()
	defer c.sending.Unlock()

	if c.shutdown || c.closing {
		call.DoneCall()
		call.Error = Errshutdown
		return
	}

	id, err := c.RegisterCall(call)
	if err != nil {
		call.DoneCall()
		call.Error = err
		return
	}

	c.header.ID = id
	c.header.ServiceMethod = call.ServiceMethod
	c.header.Error = ""

	if err := c.cc.Write(&c.header, call.Args); err != nil {
		c.RemoveCall(id)
		if call == nil {
			return
		}
		call.DoneCall()
		call.Error = err
		return
	}
}
func (c *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10) // 默认容量为10的channel
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}

	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)
	return call
}

func (c *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-c.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
