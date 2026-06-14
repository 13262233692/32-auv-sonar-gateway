package tcp

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"auv-sonar-gateway/internal/framing"
	"auv-sonar-gateway/internal/model"
	"auv-sonar-gateway/internal/uper"
)

type SonarTCPServer struct {
	listenAddr  string
	listener    *net.Listener
	conns       map[string]net.Conn
	onPing      func(*model.SonarPing)
	running     atomic.Bool
	wg          sync.WaitGroup
	mu          sync.Mutex
	stats       TCPStats
	maxConns    int
	bufferSize  int
	debugLog    bool
}

type TCPStats struct {
	ConnectionsOpened uint64
	ConnectionsClosed uint64
	BytesReceived     uint64
	PacketsReceived   uint64
	DecodeErrors      uint64
	AssemblerStats    framing.AssemblerStats
}

type clientSession struct {
	conn      net.Conn
	assembler *framing.SafeFrameProcessor
	server    *SonarTCPServer
	remoteAddr string
}

func NewSonarTCPServer(listenAddr string, maxConns, bufferSize int) *SonarTCPServer {
	return &SonarTCPServer{
		listenAddr: listenAddr,
		maxConns:   maxConns,
		bufferSize: bufferSize,
		conns:      make(map[string]net.Conn),
	}
}

func (s *SonarTCPServer) SetPingHandler(handler func(*model.SonarPing)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onPing = handler
}

func (s *SonarTCPServer) SetDebug(enabled bool) {
	s.debugLog = enabled
}

func (s *SonarTCPServer) Start() error {
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("TCP server already running")
	}

	listener, err := net.Listen("tcp4", s.listenAddr)
	if err != nil {
		s.running.Store(false)
		return fmt.Errorf("listen TCP %s: %w", s.listenAddr, err)
	}

	s.listener = &listener

	s.wg.Add(1)
	go s.acceptLoop()

	log.Printf("sonar TCP server started on %s (max_conns=%d, sliding_window=enabled)", s.listenAddr, s.maxConns)
	return nil
}

func (s *SonarTCPServer) Stop() {
	if !s.running.CompareAndSwap(true, false) {
		return
	}

	if s.listener != nil {
		(*s.listener).Close()
	}

	s.mu.Lock()
	for addr, conn := range s.conns {
		conn.Close()
		delete(s.conns, addr)
		s.stats.ConnectionsClosed++
	}
	s.mu.Unlock()

	s.wg.Wait()
	log.Printf("sonar TCP server stopped")
}

func (s *SonarTCPServer) Stats() TCPStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *SonarTCPServer) acceptLoop() {
	defer s.wg.Done()

	listener := *s.listener
	for s.running.Load() {
		conn, err := listener.Accept()
		if err != nil {
			if s.running.Load() {
				log.Printf("TCP accept error: %v", err)
			}
			continue
		}

		remoteAddr := conn.RemoteAddr().String()

		s.mu.Lock()
		if len(s.conns) >= s.maxConns {
			s.mu.Unlock()
			log.Printf("TCP connection refused: max connections %d reached (from %s)", s.maxConns, remoteAddr)
			conn.Close()
			continue
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true)
			tcpConn.SetReadBuffer(s.bufferSize)
			tcpConn.SetKeepAlive(true)
		}

		s.conns[remoteAddr] = conn
		s.stats.ConnectionsOpened++
		s.mu.Unlock()

		log.Printf("TCP connection accepted from %s", remoteAddr)

		s.wg.Add(1)
		go s.handleConnection(conn, remoteAddr)
	}
}

func (s *SonarTCPServer) handleConnection(conn net.Conn, remoteAddr string) {
	defer func() {
		conn.Close()
		s.wg.Done()
		s.mu.Lock()
		if _, ok := s.conns[remoteAddr]; ok {
			delete(s.conns, remoteAddr)
			s.stats.ConnectionsClosed++
		}
		s.mu.Unlock()
		log.Printf("TCP connection closed from %s", remoteAddr)
	}()

	assembler := framing.NewSafeSonarProcessor(uper.DecodeRawPacket, framing.MaxFrameSize)
	assembler.SetPingHandler(s.onPing)
	assembler.SetDebug(s.debugLog)

	buf := make([]byte, 32768)
	for s.running.Load() {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF && s.running.Load() {
				log.Printf("TCP read error from %s: %v", remoteAddr, err)
			}
			return
		}

		if n <= 0 {
			continue
		}

		s.mu.Lock()
		s.stats.BytesReceived += uint64(n)
		s.stats.PacketsReceived++
		s.mu.Unlock()

		if s.debugLog {
			log.Printf("[tcp] received %d bytes from %s", n, remoteAddr)
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		if err := assembler.Feed(data); err != nil {
			s.mu.Lock()
			s.stats.DecodeErrors++
			s.mu.Unlock()
			log.Printf("TCP sliding window error from %s: %v", remoteAddr, err)
		}
	}
}
