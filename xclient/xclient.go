package xclient

import (
	"context"
	"io"
	. "microrpc"
	"sync"
)

type XClient struct {
	mutex   sync.Mutex
	d       Discovery
	mode    SelectMode
	opt     *Option
	clients map[string]*Client
}

var _ io.Closer = (*XClient)(nil)

func (c *XClient) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for k, v := range c.clients {
		_ = v.Close()
		delete(c.clients, k)
	}
	return nil
}

func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*Client),
	}
}

/*
1.检查 c.clients 是否有缓存的 Client，如果有，检查是否是可用状态，如果是则返回缓存的 Client，如果不可用，则从缓存中删除。
2.如果步骤 1 没有返回缓存的 Client，则说明需要创建新的 Client，缓存并返回。
*/
func (c *XClient) dial(addr string) (*Client, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	client, ok := c.clients[addr]
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(c.clients, addr)
		client = nil
	}
	if client == nil {
		var err error
		client, err = XDial(addr, c.opt)
		if err != nil {
			return nil, err
		}
		c.clients[addr] = client
	}
	return client, nil
}

func (c *XClient) call(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := c.dial(addr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

func (c *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAdde, err := c.d.Get(c.mode)
	if err != nil {
		return err
	}
	return c.call(rpcAdde, ctx, serviceMethod, args, reply)
}
