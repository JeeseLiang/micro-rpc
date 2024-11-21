package xclient

import (
	"context"
	"errors"
	"math/rand/v2"
	"reflect"
	"sync"
)

type SelectMode int

const (
	Random SelectMode = iota
	RoundRobin
	WeightedRoundRobin
	ConsistentHash
)

type Discovery interface {
	/*
		Refresh() 从注册中心更新服务列表
		Update(servers []string) 手动更新服务列表
		Get(mode SelectMode) 根据负载均衡策略，选择一个服务实例
		GetAll() 返回所有的服务实例
	*/
	Refresh() error
	Get(key SelectMode) (string, error)
	GetAll() ([]string, error)
	Update(servers []string) error
}

var _ Discovery = (*MultiDiscovery)(nil)

type MultiDiscovery struct {
	mutex   sync.RWMutex
	servers []string
	idx     int // 记录轮询到的位置
}

func NewMultiDiscovery(servers []string) *MultiDiscovery {
	return &MultiDiscovery{
		servers: servers,
		idx:     int(rand.Int32()),
		mutex:   sync.RWMutex{},
	}
}

// 随机选择算法不需要这个方法
func (d *MultiDiscovery) Refresh() error {
	return nil
}

func (d *MultiDiscovery) Get(key SelectMode) (string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no server available")
	}
	switch key {
	case Random:
		return d.servers[rand.Int32N(int32(n))], nil
	case RoundRobin:
		server := d.servers[d.idx%n]
		d.idx = (d.idx + 1) % n
		return server, nil
	default:
		return "", errors.New("rpc discovery: unsupported select mode")
	}
}

func (d *MultiDiscovery) GetAll() ([]string, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	servers := make([]string, len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}

func (d *MultiDiscovery) Update(servers []string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.servers = servers
	return nil
}

func (c *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := c.d.GetAll()
	if err != nil {
		return err
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var e error
	replyDone := false
	ctx, cancel := context.WithCancel(ctx)
	for _, server := range servers {
		wg.Add(1)
		go func(server string) {
			defer wg.Done()
			var clonedReply interface{}

			if reply != nil {
				clonedReply = reflect.New(reflect.TypeOf(reply).Elem()).Interface()
			}

			err := c.call(server, ctx, serviceMethod, args, clonedReply)
			// 广播发现错误后立刻退出
			mu.Lock()
			if err != nil {
				e = err
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(server)
	}

	wg.Wait()
	return e
}
