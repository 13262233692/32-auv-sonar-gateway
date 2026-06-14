package framing

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"auv-sonar-gateway/internal/model"
)

const (
	INSFrameSize    = 32
	INSMaxFrameSize = 128
	INSValidRangeUS = uint64(365 * 24 * 3600 * 1e6)
)

type INSSafeProcessor struct {
	buf          []byte
	capacity     int
	readPos      int
	writePos     int
	maxSize      int
	expectedSize int

	latestTS     uint64
	stats        INSStats
	mu           sync.Mutex

	onData    func(*model.INSData)
	debugLog  bool
	running   bool
}

type INSStats struct {
	BytesFed        uint64
	FramesCompleted uint64
	ResyncEvents    uint64
	InvalidLength   uint64
	BadTimestamp    uint64
	Overruns        uint64
}

func NewINSSafeProcessor() *INSSafeProcessor {
	capacity := INSMaxFrameSize * 8
	return &INSSafeProcessor{
		buf:          make([]byte, capacity),
		capacity:     capacity,
		maxSize:      INSMaxFrameSize,
		expectedSize: INSFrameSize,
		latestTS:     0,
	}
}

func (p *INSSafeProcessor) SetDataHandler(handler func(*model.INSData)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onData = handler
}

func (p *INSSafeProcessor) SetDebug(enabled bool) {
	p.debugLog = enabled
}

func (p *INSSafeProcessor) Stats() INSStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

func (p *INSSafeProcessor) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readPos = 0
	p.writePos = 0
	p.latestTS = 0
}

func (p *INSSafeProcessor) available() int {
	return p.writePos - p.readPos
}

func (p *INSSafeProcessor) compactIfNeeded() {
	if p.readPos > 0 && p.available() < p.capacity/4 {
		if p.available() > 0 {
			copy(p.buf, p.buf[p.readPos:p.writePos])
		}
		p.writePos -= p.readPos
		p.readPos = 0
	}
}

func (p *INSSafeProcessor) Feed(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(data) == 0 {
		return nil
	}

	if p.writePos+len(data) > p.capacity {
		p.compactIfNeeded()
		if p.writePos+len(data) > p.capacity {
			p.stats.Overruns++
			resetPos := p.writePos % p.expectedSize
			if resetPos > 0 {
				copy(p.buf, p.buf[p.writePos-resetPos:p.writePos])
				p.readPos = 0
				p.writePos = resetPos
			} else {
				p.readPos = 0
				p.writePos = 0
			}
			return fmt.Errorf("INS buffer overrun, resetting")
		}
	}

	copy(p.buf[p.writePos:], data)
	p.writePos += len(data)
	p.stats.BytesFed += uint64(len(data))

	return p.processLocked()
}

func (p *INSSafeProcessor) processLocked() error {
	for p.available() >= p.expectedSize {
		frame := p.buf[p.readPos : p.readPos+p.expectedSize]

		ts := binary.LittleEndian.Uint64(frame[0:8])

		if !p.isValidTimestamp(ts) {
			p.stats.BadTimestamp++
			skip := p.findNextCandidateLocked()
			if skip <= 0 {
				break
			}
			p.readPos += skip
			p.stats.ResyncEvents++
			continue
		}

		ins, err := p.parseAndValidateFrame(frame)
		if err != nil {
			p.stats.InvalidLength++
			p.readPos++
			p.stats.ResyncEvents++
			continue
		}

		p.latestTS = ts
		p.readPos += p.expectedSize
		p.stats.FramesCompleted++

		if p.debugLog {
			fmt.Printf("[ins-proc] frame ok: ts=%d, lat=%.7f, lon=%.7f\n",
				ins.TimestampUS, ins.Latitude(), ins.Longitude())
		}

		if p.onData != nil {
			p.onData(ins)
		}
	}

	return nil
}

func (p *INSSafeProcessor) isValidTimestamp(ts uint64) bool {
	if ts == 0 {
		return false
	}

	now := uint64(time.Now().UnixNano() / 1000)

	if p.latestTS > 0 {
		diff := int64(ts) - int64(p.latestTS)
		if diff < -10*1e6 || diff > 60*1e6 {
			return false
		}
	} else {
		if ts < now-INSValidRangeUS || ts > now+60*1e6 {
			return false
		}
	}

	return true
}

func (p *INSSafeProcessor) findNextCandidateLocked() int {
	available := p.available()
	for offset := 1; offset+8 <= available; offset++ {
		ts := binary.LittleEndian.Uint64(p.buf[p.readPos+offset : p.readPos+offset+8])
		if p.isValidTimestamp(ts) {
			return offset
		}
	}
	return -1
}

func (p *INSSafeProcessor) parseAndValidateFrame(frame []byte) (*model.INSData, error) {
	if len(frame) < INSFrameSize {
		return nil, fmt.Errorf("INS frame too short: %d bytes", len(frame))
	}

	ins := &model.INSData{}

	ins.TimestampUS = binary.LittleEndian.Uint64(frame[0:8])
	ins.LatDeg7 = int64(binary.LittleEndian.Uint64(frame[8:16]))
	ins.LonDeg7 = int64(binary.LittleEndian.Uint64(frame[16:24]))
	ins.DepthMM = int32(binary.LittleEndian.Uint32(frame[24:28]))
	ins.RollCentidg = int16(binary.LittleEndian.Uint16(frame[28:30]))
	ins.PitchCentidg = int16(binary.LittleEndian.Uint16(frame[30:32]))

	if len(frame) >= 36 {
		ins.HeadingCentidg = int16(binary.LittleEndian.Uint16(frame[32:34]))
		ins.HeaveMM = int16(binary.LittleEndian.Uint16(frame[34:36]))
	}

	if len(frame) >= 42 {
		ins.VelXMM = int16(binary.LittleEndian.Uint16(frame[36:38]))
		ins.VelYMM = int16(binary.LittleEndian.Uint16(frame[38:40]))
		ins.VelZMM = int16(binary.LittleEndian.Uint16(frame[40:42]))
	}

	lat := ins.Latitude()
	lon := ins.Longitude()
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return nil, fmt.Errorf("invalid lat/lon: %.7f, %.7f", lat, lon)
	}

	roll := float64(ins.RollCentidg) / 100.0
	pitch := float64(ins.PitchCentidg) / 100.0
	if roll < -180 || roll > 180 || pitch < -90 || pitch > 90 {
		return nil, fmt.Errorf("invalid attitude: roll=%.2f, pitch=%.2f", roll, pitch)
	}

	return ins, nil
}
