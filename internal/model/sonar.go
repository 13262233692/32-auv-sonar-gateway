package model

type SonarPing struct {
	SyncPattern      uint32
	PacketLength     uint16
	PingCounter      uint16
	TimestampUS      uint64
	SoundSpeedDM     uint16
	NumBeams         uint16
	TiltAlongTrack   int16
	TiltAcrossTrack  int16
	SamplingFreqHz   uint32
	PulseLengthUS    uint16
	TransducerDepth  uint16
	Beams            []BeamData
}

type BeamData struct {
	BeamIndex      uint16
	PointingAngle  int16
	TravelTimeUS   uint16
	IntensityDB    uint8
	QualityFactor  uint8
	DetectInfo     uint8
	Reserved       uint8
}

const (
	SyncMagic     uint32 = 0x534F4E52
	MaxBeamCount uint16 = 512
	PacketHeaderBitLen  = 160
	BeamEntryBitLen     = 80
)

func (p *SonarPing) TimestampSeconds() float64 {
	return float64(p.TimestampUS) / 1e6
}

func (p *SonarPing) SoundSpeedMs() float64 {
	return float64(p.SoundSpeedDM) / 10.0
}

func (b *BeamData) AngleRad() float64 {
	return float64(b.PointingAngle) / 100.0 * (3.14159265358979323846 / 180.0)
}

func (b *BeamData) TravelTimeSec() float64 {
	return float64(b.TravelTimeUS) / 1e6
}
