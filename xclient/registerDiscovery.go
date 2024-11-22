package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type RegisterDiscovery struct {
	*MultiDiscovery
	timeout  time.Duration // timeout for register list
	registry string        // registry address
	lastTime time.Time     // last time of register list update
}

const (
	DefaultUpdateTimeout = 10 * time.Second
)

func NewRegisterDiscovery(registry string, timeout time.Duration) *RegisterDiscovery {
	if timeout == 0 {
		timeout = DefaultUpdateTimeout
	}
	return &RegisterDiscovery{
		MultiDiscovery: NewMultiDiscovery(make([]string, 0)),
		timeout:        timeout,
		registry:       registry,
		lastTime:       time.Now(),
	}
}

func (rd *RegisterDiscovery) Update(servers []string) error {
	rd.mutex.Lock()
	defer rd.mutex.Unlock()
	rd.servers = servers
	rd.lastTime = time.Now()
	return nil
}

func (rd *RegisterDiscovery) Refresh() error {
	rd.mutex.Lock()
	defer rd.mutex.Unlock()

	// fmt.Println(time.Since(rd.lastTime), rd.timeout)

	if time.Since(rd.lastTime) < rd.timeout {
		return nil
	}

	log.Println("rpc register discovery refresh")
	resp, err := http.Get(rd.registry)
	if err != nil {
		log.Println("rpc register discovery refresh error:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Micro-RPC-Servers"), ",")
	rd.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			rd.servers = append(rd.servers, strings.TrimSpace(server))
		}
	}
	rd.lastTime = time.Now()
	return nil
}

func (rd *RegisterDiscovery) GetAll() ([]string, error) {
	if err := rd.Refresh(); err != nil {
		return nil, err
	}
	return rd.MultiDiscovery.GetAll()
}

func (rd *RegisterDiscovery) Get(mode SelectMode) (string, error) {
	if err := rd.Refresh(); err != nil {
		return "", err
	}
	return rd.MultiDiscovery.Get(mode)
}
