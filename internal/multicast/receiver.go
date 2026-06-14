package multicast

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"auv-sonar-gateway/internal/model"
	"auv-sonar-gateway/internal/uper"
)

type SonarReceiver struct {
	iface      *net.Interface
	groupAddr  *net.UDPAddr
	conn       *net.UDPConn
	onPing     func(*model.SonarPing)
	running    atomic.Bool
	wg         sync.WaitGroup
	bufferSize int
	stats      ReceiverStats
	mu         sync.Mutex
}

type ReceiverStats struct {
	PacketsReceived uint64
	PacketsDropped  uint64
	DecodeErrors    uint64
	BytesReceived   uint64
}

func NewSonarReceiver(ifaceName, multicastGroup string, port int, bufferSize int) (*SonarReceiver, error) {
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return nil, fmt.Errorf("find interface %s: %w", ifaceName, err)
	}

	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", multicastGroup, port))
	if err != nil {
		return nil, fmt.Errorf("resolve multicast address: %w", err)
	}

	return &SonarReceiver{
		iface:      iface,
		groupAddr:  addr,
		bufferSize: bufferSize,
	}, nil
}

func (r *SonarReceiver) SetPingHandler(handler func(*model.SonarPing)) {
	r.onPing = handler
}

func (r *SonarReceiver) Start() error {
	if !r.running.CompareAndSwap(false, true) {
		return fmt.Errorf("receiver already running")
	}

	conn, err := net.ListenMulticastUDP("udp4", r.iface, r.groupAddr)
	if err != nil {
		r.running.Store(false)
		return fmt.Errorf("listen multicast UDP: %w", err)
	}

	if err := conn.SetReadBuffer(r.bufferSize); err != nil {
		log.Printf("warning: failed to set read buffer size: %v", err)
	}

	r.conn = conn

	r.wg.Add(1)
	go r.receiveLoop()

	log.Printf("sonar multicast receiver started on %s (interface: %s)", r.groupAddr, r.iface.Name)
	return nil
}

func (r *SonarReceiver) Stop() {
	if !r.running.CompareAndSwap(true, false) {
		return
	}
	if r.conn != nil {
		r.conn.Close()
	}
	r.wg.Wait()
	log.Printf("sonar multicast receiver stopped")
}

func (r *SonarReceiver) GetStats() ReceiverStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

func (r *SonarReceiver) receiveLoop() {
	defer r.wg.Done()

	buf := make([]byte, 65535)
	for r.running.Load() {
		n, src, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			if r.running.Load() {
				log.Printf("read error from %s: %v", src, err)
			}
			continue
		}

		r.mu.Lock()
		r.stats.PacketsReceived++
		r.stats.BytesReceived += uint64(n)
		r.mu.Unlock()

		data := make([]byte, n)
		copy(data, buf[:n])

		ping, err := uper.DecodeRawPacket(data)
		if err != nil {
			r.mu.Lock()
			r.stats.DecodeErrors++
			r.mu.Unlock()
			log.Printf("decode error: %v", err)
			continue
		}

		if r.onPing != nil {
			r.onPing(ping)
		}
	}
}
