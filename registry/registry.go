package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultTimeout = 5 * time.Minute // 默认过期时间为5min
	DefaultPath    = "/_micro_rpc_/registry"
)

var (
	DefaultRegistry = NewRegistry(DefaultTimeout)
)

type Registry struct {
	mutex    sync.Mutex
	services map[string]*ServerInfo
	timeout  time.Duration
}

type ServerInfo struct {
	Addr  string
	Start time.Time
}

func NewRegistry(timeout time.Duration) *Registry {
	return &Registry{
		services: make(map[string]*ServerInfo),
		timeout:  timeout,
	}
}

func (r *Registry) Register(addr string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	s, ok := r.services[addr]
	if ok {
		s.Start = time.Now()
	} else {
		r.services[addr] = &ServerInfo{
			Addr:  addr,
			Start: time.Now(),
		}
	}
}

func (r *Registry) aliveServers() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var servers []string
	for addr, s := range r.services {
		if r.timeout == 0 || s.Start.Add(r.timeout).After(time.Now()) {
			servers = append(servers, addr)
		} else {
			delete(r.services, addr)
		}
	}
	sort.Strings(servers)
	return servers
}

// 为了实现上的简单，采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中。
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-Micro-RPC-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-Micro-RPC-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.Register(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Registry) HandleHTTP(path string) {
	http.Handle(path, r)
	log.Println("rpc registry path:", path)
}

func HandleHTTP() {
	DefaultRegistry.HandleHTTP(DefaultPath)
}

func Heartbeat(registry, addr string, timeout time.Duration) {
	if timeout == 0 {
		// 若未设置发送心跳时间，则使用默认值减1min
		// 确保在默认超时时间内，服务端能够收到正常的心跳
		timeout = DefaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		ticker := time.NewTicker(timeout)
		for err == nil {
			<-ticker.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heartbeat to registry:", registry)
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Micro-RPC-Server", addr)
	httpClient := &http.Client{}
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: send heartbeat to registry failed:", err)
		return err
	}
	return nil
}
