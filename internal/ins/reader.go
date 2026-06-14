package ins

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"auv-sonar-gateway/internal/model"
	"auv-sonar-gateway/internal/uper"
)

type INSReader struct {
	addr      *net.UDPAddr
	conn      *net.UDPConn
	onData    func(*model.INSData)
	running   atomic.Bool
	wg        sync.WaitGroup
	latest    *model.INSData
	mu        sync.RWMutex
}

func NewINSReader(listenAddr string, port int) (*INSReader, error) {
	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", listenAddr, port))
	if err != nil {
		return nil, fmt.Errorf("resolve INS address: %w", err)
	}
	return &INSReader{
		addr: addr,
	}, nil
}

func (r *INSReader) SetDataHandler(handler func(*model.INSData)) {
	r.onData = handler
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

	log.Printf("INS reader started on %s", r.addr)
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

func (r *INSReader) receiveLoop() {
	defer r.wg.Done()

	buf := make([]byte, 1024)
	for r.running.Load() {
		n, _, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			if r.running.Load() {
				log.Printf("INS read error: %v", err)
			}
			continue
		}

		ins, err := uper.DecodeINSFromBytes(buf[:n])
		if err != nil {
			log.Printf("INS decode error: %v", err)
			continue
		}

		r.mu.Lock()
		r.latest = ins
		r.mu.Unlock()

		if r.onData != nil {
			r.onData(ins)
		}
	}
}
