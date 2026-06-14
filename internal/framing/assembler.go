package framing

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"auv-sonar-gateway/internal/model"
)

const (
	MaxFrameSize     = 65535
	MinFrameSize     = 40
	SyncWordSize     = 4
	HeaderByteSize   = 20
	SyncMagicBytes   = 0x534F4E52
	MaxResyncAttempts = 16
)

type FrameState int

const (
	StateWaitSync FrameState = iota
	StateReadHeader
	StateReadPayload
	StateFrameReady
)

func (s FrameState) String() string {
	switch s {
	case StateWaitSync:
		return "WAIT_SYNC"
	case StateReadHeader:
		return "READ_HEADER"
	case StateReadPayload:
		return "READ_PAYLOAD"
	case StateFrameReady:
		return "FRAME_READY"
	default:
		return "UNKNOWN"
	}
}

type AssemblerStats struct {
	BytesFed       uint64
	SyncFound      uint64
	HeaderParsed   uint64
	FramesCompleted uint64
	ResyncEvents   uint64
	InvalidLength  uint64
	Overruns       uint64
	Truncated      uint64
}

type SlidingWindowAssembler struct {
	buf       []byte
	capacity  int
	readPos   int
	writePos  int
	state     FrameState
	maxSize   int
	minSize   int

	expectedPayloadLen int
	packetLenField     uint16

	syncScanOffset int

	stats AssemblerStats
	mu    sync.Mutex

	debugLog   bool
	onFrame    func([]byte) error
	onError    func(error)
}

func NewSlidingWindowAssembler(maxSize int) *SlidingWindowAssembler {
	if maxSize <= 0 {
		maxSize = MaxFrameSize
	}
	capacity := maxSize * 4
	return &SlidingWindowAssembler{
		buf:      make([]byte, capacity),
		capacity: capacity,
		maxSize:  maxSize,
		minSize:  MinFrameSize,
		state:    StateWaitSync,
	}
}

func (a *SlidingWindowAssembler) SetFrameHandler(handler func([]byte) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.onFrame = handler
}

func (a *SlidingWindowAssembler) SetErrorHandler(handler func(error)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.onError = handler
}

func (a *SlidingWindowAssembler) SetDebug(enabled bool) {
	a.debugLog = enabled
}

func (a *SlidingWindowAssembler) Stats() AssemblerStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}

func (a *SlidingWindowAssembler) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.resetLocked()
}

func (a *SlidingWindowAssembler) resetLocked() {
	a.readPos = 0
	a.writePos = 0
	a.state = StateWaitSync
	a.expectedPayloadLen = 0
	a.packetLenField = 0
	a.syncScanOffset = 0
}

func (a *SlidingWindowAssembler) available() int {
	return a.writePos - a.readPos
}

func (a *SlidingWindowAssembler) compactIfNeeded() {
	if a.readPos > 0 && a.available() < a.capacity/4 {
		if a.available() > 0 {
			copy(a.buf, a.buf[a.readPos:a.writePos])
		}
		a.writePos -= a.readPos
		a.readPos = 0
	}
}

func (a *SlidingWindowAssembler) Feed(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(data) == 0 {
		return nil
	}

	if a.writePos+len(data) > a.capacity {
		a.compactIfNeeded()
		if a.writePos+len(data) > a.capacity {
			a.stats.Overruns++
			if a.onError != nil {
				a.onError(fmt.Errorf("buffer overrun: writePos=%d, len(data)=%d, capacity=%d, dropping %d bytes",
					a.writePos, len(data), a.capacity, len(data)))
			}
			a.resetLocked()
			return nil
		}
	}

	copy(a.buf[a.writePos:], data)
	a.writePos += len(data)
	a.stats.BytesFed += uint64(len(data))

	return a.runStateMachineLocked()
}

func (a *SlidingWindowAssembler) runStateMachineLocked() error {
	for {
		switch a.state {
		case StateWaitSync:
			if !a.handleWaitSyncLocked() {
				return nil
			}

		case StateReadHeader:
			if !a.handleReadHeaderLocked() {
				return nil
			}

		case StateReadPayload:
			done, err := a.handleReadPayloadLocked()
			if err != nil {
				return err
			}
			if !done {
				return nil
			}

		case StateFrameReady:
			a.state = StateWaitSync
			a.syncScanOffset = 0
		}
	}
}

func (a *SlidingWindowAssembler) handleWaitSyncLocked() bool {
	available := a.available()
	if available < SyncWordSize {
		return false
	}

	scanStart := a.readPos + a.syncScanOffset
	maxScan := a.writePos - SyncWordSize

	for i := scanStart; i <= maxScan; i++ {
		sync := binary.BigEndian.Uint32(a.buf[i : i+SyncWordSize])
		if sync == SyncMagicBytes {
			a.readPos = i
			a.syncScanOffset = 0
			a.state = StateReadHeader
			a.stats.SyncFound++
			if a.debugLog {
				fmt.Printf("[framing] sync found at offset %d, state -> READ_HEADER\n", a.readPos)
			}
			return true
		}
	}

	if a.writePos > a.capacity/2 {
		a.stats.ResyncEvents++
		if a.onError != nil {
			a.onError(fmt.Errorf("sync word not found after scanning %d bytes, resetting",
				a.available()))
		}
		a.resetLocked()
	} else {
		a.syncScanOffset = maxScan - a.readPos + 1
	}

	return false
}

func (a *SlidingWindowAssembler) handleReadHeaderLocked() bool {
	available := a.available()
	if available < HeaderByteSize {
		return false
	}

	br := NewByteReader(a.buf[a.readPos : a.readPos+HeaderByteSize])

	sync, _ := br.ReadUint32BE()
	pktLen, _ := br.ReadUint16BE()
	_, _ = br.ReadUint16BE()
	_, _ = br.ReadUint64BE()
	_, _ = br.ReadUint16BE()

	_ = sync

	if pktLen < uint16(a.minSize) || pktLen > uint16(a.maxSize) {
		a.stats.InvalidLength++
		if a.onError != nil {
			a.onError(fmt.Errorf("invalid packet length %d (valid range [%d, %d]), triggering resync",
				pktLen, a.minSize, a.maxSize))
		}
		a.readPos++
		a.stats.ResyncEvents++
		a.state = StateWaitSync
		a.syncScanOffset = 0
		return true
	}

	a.packetLenField = pktLen
	a.expectedPayloadLen = int(pktLen)

	a.stats.HeaderParsed++
	a.state = StateReadPayload

	if a.debugLog {
		fmt.Printf("[framing] header parsed at offset %d: pktLen=%d, state -> READ_PAYLOAD\n",
			a.readPos, pktLen)
	}

	return true
}

func (a *SlidingWindowAssembler) handleReadPayloadLocked() (bool, error) {
	available := a.available()
	if available < a.expectedPayloadLen {
		return false, nil
	}

	frame := make([]byte, a.expectedPayloadLen)
	copy(frame, a.buf[a.readPos:a.readPos+a.expectedPayloadLen])

	frameEnd := a.readPos + a.expectedPayloadLen
	a.readPos = frameEnd

	a.stats.FramesCompleted++
	a.state = StateFrameReady

	if a.debugLog {
		fmt.Printf("[framing] frame completed at offset %d: len=%d, state -> FRAME_READY\n",
			a.readPos-a.expectedPayloadLen, a.expectedPayloadLen)
	}

	if a.onFrame != nil {
		if err := a.onFrame(frame); err != nil {
			if a.onError != nil {
				a.onError(fmt.Errorf("frame handler error: %w", err))
			}
			return false, err
		}
	}

	return true, nil
}

func (a *SlidingWindowAssembler) DrainTo(w io.Writer) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.available() == 0 {
		return 0, nil
	}

	n, err := w.Write(a.buf[a.readPos:a.writePos])
	if err != nil {
		return n, err
	}
	a.readPos += n
	return n, nil
}

type ByteReader struct {
	data   []byte
	offset int
}

func NewByteReader(data []byte) *ByteReader {
	return &ByteReader{data: data, offset: 0}
}

func (r *ByteReader) ReadUint32BE() (uint32, error) {
	if r.offset+4 > len(r.data) {
		return 0, errors.New("byte reader: not enough data for uint32")
	}
	v := binary.BigEndian.Uint32(r.data[r.offset : r.offset+4])
	r.offset += 4
	return v, nil
}

func (r *ByteReader) ReadUint16BE() (uint16, error) {
	if r.offset+2 > len(r.data) {
		return 0, errors.New("byte reader: not enough data for uint16")
	}
	v := binary.BigEndian.Uint16(r.data[r.offset : r.offset+2])
	r.offset += 2
	return v, nil
}

func (r *ByteReader) ReadUint64BE() (uint64, error) {
	if r.offset+8 > len(r.data) {
		return 0, errors.New("byte reader: not enough data for uint64")
	}
	v := binary.BigEndian.Uint64(r.data[r.offset : r.offset+8])
	r.offset += 8
	return v, nil
}

func (r *ByteReader) Remaining() int {
	return len(r.data) - r.offset
}

type FrameValidatorFunc func([]byte) error

func ValidateSonarFrame(frame []byte) error {
	if len(frame) < HeaderByteSize {
		return fmt.Errorf("frame too short: %d bytes", len(frame))
	}

	sync := binary.BigEndian.Uint32(frame[0:4])
	if sync != model.SyncMagic {
		return fmt.Errorf("invalid sync word: 0x%08X", sync)
	}

	pktLen := binary.BigEndian.Uint16(frame[4:6])
	if int(pktLen) != len(frame) {
		return fmt.Errorf("packet length mismatch: header=%d, actual=%d", pktLen, len(frame))
	}

	numBeams := binary.BigEndian.Uint16(frame[18:20])
	if numBeams > model.MaxBeamCount {
		return fmt.Errorf("beam count %d exceeds max %d", numBeams, model.MaxBeamCount)
	}

	expectedLen := HeaderByteSize + int(numBeams)*10
	if len(frame) < expectedLen {
		return fmt.Errorf("truncated frame: need %d bytes for %d beams, have %d",
			expectedLen, numBeams, len(frame))
	}

	return nil
}

type ValidatingAssembler struct {
	*SlidingWindowAssembler
	validator FrameValidatorFunc
}

func NewValidatingAssembler(maxSize int, validator FrameValidatorFunc) *ValidatingAssembler {
	va := &ValidatingAssembler{
		SlidingWindowAssembler: NewSlidingWindowAssembler(maxSize),
		validator:              validator,
	}

	va.SlidingWindowAssembler.SetFrameHandler(func(frame []byte) error {
		if va.validator != nil {
			if err := va.validator(frame); err != nil {
				return fmt.Errorf("frame validation failed: %w", err)
			}
		}
		return nil
	})

	return va
}
