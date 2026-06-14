package framing

import (
	"encoding/binary"
	"fmt"
	"sync"

	"auv-sonar-gateway/internal/model"
)

type SafeFrameProcessor struct {
	assembler    *ValidatingAssembler
	decoder      func([]byte) (*model.SonarPing, error)
	onPing       func(*model.SonarPing)
	maxConsecutiveErrors int
	errorCount   int
	mu           sync.Mutex
}

func NewSafeSonarProcessor(decoder func([]byte) (*model.SonarPing, error), maxFrameSize int) *SafeFrameProcessor {
	assembler := NewValidatingAssembler(maxFrameSize, ValidateSonarFrame)
	sp := &SafeFrameProcessor{
		assembler:    assembler,
		decoder:      decoder,
		maxConsecutiveErrors: 10,
	}

	assembler.SetFrameHandler(sp.handleValidFrame)
	assembler.SetErrorHandler(sp.handleAssemblerError)

	return sp
}

func (sp *SafeFrameProcessor) SetPingHandler(handler func(*model.SonarPing)) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.onPing = handler
}

func (sp *SafeFrameProcessor) Feed(data []byte) error {
	return sp.assembler.Feed(data)
}

func (sp *SafeFrameProcessor) handleValidFrame(frame []byte) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if len(frame) < HeaderByteSize {
		return fmt.Errorf("safe processor: frame too short %d bytes", len(frame))
	}

	statedLen := binary.BigEndian.Uint16(frame[4:6])
	if int(statedLen) != len(frame) {
		return fmt.Errorf("safe processor: length mismatch stated=%d actual=%d", statedLen, len(frame))
	}

	numBeams := binary.BigEndian.Uint16(frame[18:20])
	if numBeams > model.MaxBeamCount {
		return fmt.Errorf("safe processor: beam count %d exceeds max %d", numBeams, model.MaxBeamCount)
	}

	minExpectedLen := HeaderByteSize + int(numBeams)*10
	if len(frame) < minExpectedLen {
		return fmt.Errorf("safe processor: truncated frame need %d have %d", minExpectedLen, len(frame))
	}

	safeMaxLen := HeaderByteSize + int(model.MaxBeamCount)*10
	if len(frame) > safeMaxLen {
		return fmt.Errorf("safe processor: frame length %d exceeds safe max %d", len(frame), safeMaxLen)
	}

	ping, err := sp.decoder(frame)
	if err != nil {
		sp.errorCount++
		if sp.errorCount > sp.maxConsecutiveErrors {
			sp.assembler.Reset()
			sp.errorCount = 0
		}
		return fmt.Errorf("safe processor: decode error: %w", err)
	}

	sp.errorCount = 0

	if sp.onPing != nil {
		sp.onPing(ping)
	}

	return nil
}

func (sp *SafeFrameProcessor) handleAssemblerError(err error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.errorCount++
	if sp.errorCount > sp.maxConsecutiveErrors {
		sp.assembler.Reset()
		sp.errorCount = 0
	}
}

func (sp *SafeFrameProcessor) Stats() AssemblerStats {
	return sp.assembler.Stats()
}

func (sp *SafeFrameProcessor) Reset() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.assembler.Reset()
	sp.errorCount = 0
}

func (sp *SafeFrameProcessor) SetDebug(enabled bool) {
	sp.assembler.SetDebug(enabled)
}
