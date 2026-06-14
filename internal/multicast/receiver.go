package multicast

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"auv-sonar-gateway/internal/framing"
	"auv-sonar-gateway/internal/model"
	"auv-sonar-gateway/internal/uper"
)

type SonarReceiver struct {
	iface       *net.Interface
	groupAddr   *net.UDPAddr
	conn        *net.UDPConn
	onPing      func(*model.SonarPing)
	running     atomic.Bool
	wg          sync.WaitGroup
	bufferSize  int
	stats       ReceiverStats
	mu          sync.Mutex
	assembler   *framing.SafeFrameProcessor
	useSlidingWindow bool
	debugLog    bool
}

type ReceiverStats struct {
	PacketsReceived uint64
	PacketsDropped  uint64
	DecodeErrors    uint64
	BytesReceived   uint64
	AssemblerStats  framing.AssemblerStats
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

	r := &SonarReceiver{
		iface:       iface,
		groupAddr:   addr,
		bufferSize:  bufferSize,
		useSlidingWindow: true,
	}

	r.assembler = framing.NewSafeSonarProcessor(uper.DecodeRawPacket, framing.MaxFrameSize)

	return r, nil
}

func (r *SonarReceiver) SetPingHandler(handler func(*model.SonarPing)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onPing = handler
	if r.assembler != nil {
		r.assembler.SetPingHandler(handler)
	}
}

func (r *SonarReceiver) SetSlidingWindow(enabled bool) {
	r.useSlidingWindow = enabled
}

func (r *SonarReceiver) SetDebug(enabled bool) {
	r.debugLog = enabled
	if r.assembler != nil {
		r.assembler.SetDebug(enabled)
	}
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

	log.Printf("sonar multicast receiver started on %s (interface: %s, sliding_window=%v)",
		r.groupAddr, r.iface.Name, r.useSlidingWindow)
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
	stats := r.stats
	if r.assembler != nil {
		stats.AssemblerStats = r.assembler.Stats()
	}
	return stats
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

		if r.debugLog && n > 0 {
			log.Printf("[multicast] received %d bytes from %s", n, src)
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		if r.useSlidingWindow {
			if err := r.assembler.Feed(data); err != nil {
				r.mu.Lock()
				r.stats.DecodeErrors++
				r.mu.Unlock()
				log.Printf("sliding window feed error: %v", err)
			}
		} else {
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
}
