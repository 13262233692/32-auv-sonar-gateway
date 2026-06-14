package ins

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"auv-sonar-gateway/internal/framing"
	"auv-sonar-gateway/internal/model"
)

type INSReader struct {
	addr      *net.UDPAddr
	conn      *net.UDPConn
	onData    func(*model.INSData)
	running   atomic.Bool
	wg        sync.WaitGroup
	latest    *model.INSData
	mu        sync.RWMutex
	processor *framing.INSSafeProcessor
	debugLog  bool
}

func NewINSReader(listenAddr string, port int) (*INSReader, error) {
	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", listenAddr, port))
	if err != nil {
		return nil, fmt.Errorf("resolve INS address: %w", err)
	}
	r := &INSReader{
		addr:      addr,
		processor: framing.NewINSSafeProcessor(),
	}
	return r, nil
}

func (r *INSReader) SetDataHandler(handler func(*model.INSData)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onData = handler
	if r.processor != nil {
		r.processor.SetDataHandler(func(ins *model.INSData) {
			r.mu.Lock()
			r.latest = ins
			r.mu.Unlock()
			if r.onData != nil {
				r.onData(ins)
			}
		})
	}
}

func (r *INSReader) SetDebug(enabled bool) {
	r.debugLog = enabled
	if r.processor != nil {
		r.processor.SetDebug(enabled)
	}
}

func (r *INSReader) Start() error {
	if !r.running.CompareAndSwap(false, true) {
		return fmt.Errorf("INS reader already running")
	}

	conn, err := net.ListenUDP("udp4", r.addr)
	if err != nil {
		r.running.Store(false)
		return fmt.Errorf("listen INS UDP: %w", err)
	}
	r.conn = conn

	r.wg.Add(1)
	go r.receiveLoop()

	log.Printf("INS reader started on %s (sliding_window=enabled)", r.addr)
	return nil
}

func (r *INSReader) Stop() {
	if !r.running.CompareAndSwap(true, false) {
		return
	}
	if r.conn != nil {
		r.conn.Close()
	}
	r.wg.Wait()
	log.Printf("INS reader stopped")
}

func (r *INSReader) Latest() *model.INSData {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.latest
}

func (r *INSReader) Stats() framing.INSStats {
	if r.processor != nil {
		return r.processor.Stats()
	}
	return framing.INSStats{}
}

func (r *INSReader) receiveLoop() {
	defer r.wg.Done()

	buf := make([]byte, 4096)
	for r.running.Load() {
		n, _, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			if r.running.Load() {
				log.Printf("INS read error: %v", err)
			}
			continue
		}

		if r.debugLog && n > 0 {
			log.Printf("[ins] received %d bytes", n)
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		if err := r.processor.Feed(data); err != nil {
			log.Printf("INS processor error: %v", err)
		}
	}
}
