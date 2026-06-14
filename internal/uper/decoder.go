package uper

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"auv-sonar-gateway/internal/model"
)

type BitReader struct {
	data   []byte
	offset uint64
	len    uint64
}

func NewBitReader(data []byte) *BitReader {
	return &BitReader{
		data:   data,
		offset: 0,
		len:    uint64(len(data)) * 8,
	}
}

func (r *BitReader) BitsRemaining() uint64 {
	if r.offset >= r.len {
		return 0
	}
	return r.len - r.offset
}

func (r *BitReader) ReadBits(n uint) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	if r.offset+uint64(n) > r.len {
		return 0, fmt.Errorf("bit reader overflow: need %d bits, have %d remaining", n, r.BitsRemaining())
	}

	var result uint64
	remaining := n

	for remaining > 0 {
		byteIdx := r.offset / 8
		bitIdx := r.offset % 8
		bitsInCurrentByte := uint(8 - bitIdx)
		bitsToRead := remaining
		if bitsToRead > bitsInCurrentByte {
			bitsToRead = bitsInCurrentByte
		}

		shift := bitsInCurrentByte - bitsToRead
		mask := byte((1 << bitsToRead) - 1)
		extracted := (r.data[byteIdx] >> shift) & mask

		result = (result << bitsToRead) | uint64(extracted)
		r.offset += uint64(bitsToRead)
		remaining -= bitsToRead
	}

	return result, nil
}

func (r *BitReader) ReadUint(n uint) (uint64, error) {
	return r.ReadBits(n)
}

func (r *BitReader) ReadInt(n uint) (int64, error) {
	val, err := r.ReadBits(n)
	if err != nil {
		return 0, err
	}
	if n > 0 && (val&(1<<(n-1))) != 0 {
		val |= ^((1 << n) - 1)
	}
	return int64(val), nil
}

func (r *BitReader) ReadConstrainedWholeNumber(min, max uint64) (uint64, error) {
	rangeVal := max - min
	if rangeVal == 0 {
		return min, nil
	}
	nBits := uint(bitLen(rangeVal))
	val, err := r.ReadBits(nBits)
	if err != nil {
		return 0, err
	}
	return min + val, nil
}

func (r *BitReader) ReadBitField(n uint) ([]byte, error) {
	totalBits := n
	totalBytes := (totalBits + 7) / 8
	buf := make([]byte, totalBytes)

	for i := uint(0); i < n; i++ {
		bit, err := r.ReadBits(1)
		if err != nil {
			return nil, err
		}
		byteIdx := i / 8
		bitIdx := 7 - (i % 8)
		buf[byteIdx] |= byte(bit << bitIdx)
	}
	return buf, nil
}

func (r *BitReader) ByteAlign() {
	rem := r.offset % 8
	if rem != 0 {
		r.offset += (8 - rem)
	}
}

func (r *BitReader) Offset() uint64 {
	return r.offset
}

func bitLen(v uint64) int {
	n := 0
	for v > 0 {
		n++
		v >>= 1
	}
	return n
}

type SonarPacketDecoder struct {
	reader *BitReader
}

func NewSonarPacketDecoder(data []byte) *SonarPacketDecoder {
	return &SonarPacketDecoder{
		reader: NewBitReader(data),
	}
}

func (d *SonarPacketDecoder) Decode() (*model.SonarPing, error) {
	ping := &model.SonarPing{}

	sync, err := d.reader.ReadUint(32)
	if err != nil {
		return nil, fmt.Errorf("read sync pattern: %w", err)
	}
	ping.SyncPattern = uint32(sync)
	if ping.SyncPattern != model.SyncMagic {
		return nil, fmt.Errorf("invalid sync pattern: 0x%08X, expected 0x%08X", ping.SyncPattern, model.SyncMagic)
	}

	pktLen, err := d.reader.ReadUint(16)
	if err != nil {
		return nil, fmt.Errorf("read packet length: %w", err)
	}
	ping.PacketLength = uint16(pktLen)

	pingCnt, err := d.reader.ReadUint(16)
	if err != nil {
		return nil, fmt.Errorf("read ping counter: %w", err)
	}
	ping.PingCounter = uint16(pingCnt)

	ts, err := d.reader.ReadUint(64)
	if err != nil {
		return nil, fmt.Errorf("read timestamp: %w", err)
	}
	ping.TimestampUS = ts

	ss, err := d.reader.ReadUint(16)
	if err != nil {
		return nil, fmt.Errorf("read sound speed: %w", err)
	}
	ping.SoundSpeedDM = uint16(ss)

	nBeams, err := d.reader.ReadUint(16)
	if err != nil {
		return nil, fmt.Errorf("read num beams: %w", err)
	}
	ping.NumBeams = uint16(nBeams)
	if ping.NumBeams > model.MaxBeamCount {
		return nil, fmt.Errorf("beam count %d exceeds maximum %d", ping.NumBeams, model.MaxBeamCount)
	}

	tiltA, err := d.reader.ReadInt(16)
	if err != nil {
		return nil, fmt.Errorf("read tilt along-track: %w", err)
	}
	ping.TiltAlongTrack = int16(tiltA)

	tiltC, err := d.reader.ReadInt(16)
	if err != nil {
		return nil, fmt.Errorf("read tilt across-track: %w", err)
	}
	ping.TiltAcrossTrack = int16(tiltC)

	sf, err := d.reader.ReadUint(32)
	if err != nil {
		return nil, fmt.Errorf("read sampling frequency: %w", err)
	}
	ping.SamplingFreqHz = uint32(sf)

	pl, err := d.reader.ReadUint(16)
	if err != nil {
		return nil, fmt.Errorf("read pulse length: %w", err)
	}
	ping.PulseLengthUS = uint16(pl)

	td, err := d.reader.ReadUint(16)
	if err != nil {
		return nil, fmt.Errorf("read transducer depth: %w", err)
	}
	ping.TransducerDepth = uint16(td)

	ping.Beams = make([]model.BeamData, 0, ping.NumBeams)
	for i := uint16(0); i < ping.NumBeams; i++ {
		beam, err := d.decodeBeam(i)
		if err != nil {
			return nil, fmt.Errorf("decode beam %d: %w", i, err)
		}
		ping.Beams = append(ping.Beams, beam)
	}

	return ping, nil
}

func (d *SonarPacketDecoder) decodeBeam(index uint16) (model.BeamData, error) {
	var beam model.BeamData
	beam.BeamIndex = index

	angle, err := d.reader.ReadInt(16)
	if err != nil {
		return beam, fmt.Errorf("read pointing angle: %w", err)
	}
	beam.PointingAngle = int16(angle)

	tt, err := d.reader.ReadUint(16)
	if err != nil {
		return beam, fmt.Errorf("read travel time: %w", err)
	}
	beam.TravelTimeUS = uint16(tt)

	intensity, err := d.reader.ReadUint(8)
	if err != nil {
		return beam, fmt.Errorf("read intensity: %w", err)
	}
	beam.IntensityDB = uint8(intensity)

	qf, err := d.reader.ReadUint(8)
	if err != nil {
		return beam, fmt.Errorf("read quality factor: %w", err)
	}
	beam.QualityFactor = uint8(qf)

	di, err := d.reader.ReadUint(8)
	if err != nil {
		return beam, fmt.Errorf("read detect info: %w", err)
	}
	beam.DetectInfo = uint8(di)

	res, err := d.reader.ReadUint(8)
	if err != nil {
		return beam, fmt.Errorf("read reserved: %w", err)
	}
	beam.Reserved = uint8(res)

	return beam, nil
}

func DecodeRawPacket(data []byte) (*model.SonarPing, error) {
	decoder := NewSonarPacketDecoder(data)
	return decoder.Decode()
}

func DecodeINSFromBytes(data []byte) (*model.INSData, error) {
	if len(data) < 32 {
		return nil, fmt.Errorf("INS packet too short: %d bytes", len(data))
	}
	ins := &model.INSData{}
	buf := bytes.NewReader(data)

	var ts uint64
	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return nil, fmt.Errorf("read INS timestamp: %w", err)
	}
	ins.TimestampUS = ts

	if err := binary.Read(buf, binary.LittleEndian, &ins.LatDeg7); err != nil {
		return nil, fmt.Errorf("read latitude: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.LonDeg7); err != nil {
		return nil, fmt.Errorf("read longitude: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.DepthMM); err != nil {
		return nil, fmt.Errorf("read depth: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.RollCentidg); err != nil {
		return nil, fmt.Errorf("read roll: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.PitchCentidg); err != nil {
		return nil, fmt.Errorf("read pitch: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.HeadingCentidg); err != nil {
		return nil, fmt.Errorf("read heading: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.HeaveMM); err != nil {
		return nil, fmt.Errorf("read heave: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.VelXMM); err != nil {
		return nil, fmt.Errorf("read vel x: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.VelYMM); err != nil {
		return nil, fmt.Errorf("read vel y: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &ins.VelZMM); err != nil {
		return nil, fmt.Errorf("read vel z: %w", err)
	}

	return ins, nil
}
