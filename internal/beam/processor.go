package beam

import (
	"log"
	"sync"
	"sync/atomic"

	"auv-sonar-gateway/internal/coord"
	"auv-sonar-gateway/internal/ins"
	"auv-sonar-gateway/internal/model"
	"auv-sonar-gateway/internal/ssp"
)

type Processor struct {
	rayTracer   *ssp.RayTracer
	transformer *coord.TransformEngine
	insReader   *ins.INSReader
	onFrame     func(*model.PointCloudFrame)
	running     atomic.Bool
	wg          sync.WaitGroup
	mu          sync.RWMutex
	processed   uint64
	dropped     uint64
}

func NewProcessor(insR *ins.INSReader, profile *model.SoundSpeedProfile) *Processor {
	return &Processor{
		rayTracer:   ssp.NewRayTracer(profile),
		transformer: coord.NewTransformEngine(),
		insReader:   insR,
	}
}

func (p *Processor) SetFrameHandler(handler func(*model.PointCloudFrame)) {
	p.onFrame = handler
}

func (p *Processor) UpdateProfile(profile *model.SoundSpeedProfile) {
	p.rayTracer.UpdateProfile(profile)
}

func (p *Processor) ProcessPing(ping *model.SonarPing) *model.PointCloudFrame {
	if ping == nil || len(ping.Beams) == 0 {
		return nil
	}

	insData := p.insReader.Latest()
	if insData == nil {
		log.Printf("warning: no INS data available, skipping ping %d", ping.PingCounter)
		p.mu.Lock()
		p.dropped++
		p.mu.Unlock()
		return nil
	}

	frame := model.NewPointCloudFrame(ping.TimestampUS, ping.PingCounter, len(ping.Beams))

	tiltAlong := float64(ping.TiltAlongTrack) / 100.0 * coord.DegToRad
	tiltAcross := float64(ping.TiltAcrossTrack) / 100.0 * coord.DegToRad

	for i := range ping.Beams {
		beam := &ping.Beams[i]

		rayResult := p.rayTracer.Trace(beam.AngleRad(), beam.TravelTimeSec()*0.5)

		geo := p.transformer.FullTransform(
			rayResult.HorizontalOffset,
			rayResult.DepthOffset,
			tiltAlong,
			tiltAcross,
			insData,
		)

		geo.Intensity = beam.IntensityDB
		geo.TimestampUS = ping.TimestampUS
		geo.PingIndex = ping.PingCounter
		geo.BeamIndex = beam.BeamIndex

		frame.AddPoint(geo)
	}

	p.mu.Lock()
	p.processed++
	p.mu.Unlock()

	if p.onFrame != nil {
		p.onFrame(frame)
	}

	return frame
}

func (p *Processor) GetStats() (processed, dropped uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.processed, p.dropped
}
